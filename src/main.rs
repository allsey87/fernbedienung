#![warn(rust_2018_idioms)]

use std::path::PathBuf;
use std::collections::HashMap;
use std::io;

use serde::{Deserialize, Serialize};

use futures::{channel::mpsc::UnboundedSender, future::FusedFuture, prelude::*, stream::FuturesUnordered};

use tokio::net::TcpListener;
use tokio::io::AsyncWriteExt;
use tokio_serde::{SymmetricallyFramed, formats::SymmetricalJson};
use tokio_util::codec::{Decoder, FramedRead, FramedWrite, LengthDelimitedCodec};

use bytes::BytesMut;

mod process;

#[derive(Debug, Deserialize)]
struct Upload {
    filename: PathBuf,
    path: PathBuf,
    contents: Vec<u8>,
}

#[derive(Debug, Deserialize)]
struct Run {
    target: PathBuf,
    working_dir: PathBuf,
    args: Vec<String>,
}

#[derive(Debug, Deserialize)]
enum Request {
    Ping,
    Upload(Upload),
    Run(Run),
    Process(u32, process::Request),
}

#[derive(Debug, Serialize)]
enum Response {
    Ok,
    Error(String),
    Process(u32, process::Response),
}

impl<T, E: std::fmt::Display> std::convert::From<Result<T, E>> for Response {
    fn from(result: Result<T, E>) -> Self {
        match result {
            Ok(_) => Response::Ok,
            Err(error) => Response::Error(error.to_string())
        }
    }
}

struct ProcessCodec(u32, process::Source);

impl ProcessCodec {
    fn new(pid: u32, source: process::Source) -> ProcessCodec {
        ProcessCodec(pid, source)
    }
}

impl Decoder for ProcessCodec {
    type Item = Response;
    type Error = io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Response>, io::Error> {
        if !buf.is_empty() {
            let length = buf.len();
            let response = Response::Process(self.0, process::Response::Output(self.1, buf.split_to(length)));
            Ok(Some(response))
        } else {
            Ok(None)
        }
    }
}

#[tokio::main]
pub async fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("fernbedienung=info")).init();

    // Bind a server socket
    let listener = TcpListener::bind("127.0.0.1:17653").await.unwrap();

    println!("listening on {:?}", listener.local_addr());

    loop {
        let (mut socket, peer_addr) = listener.accept().await.unwrap();

        tokio::spawn(async move {
            // TODO security isn't important for this application, move this hashmap out
            // of the client and protect with RwLock?
            let mut process_tx : HashMap<u32, UnboundedSender<process::Request>> = HashMap::new();
            let mut processes = FuturesUnordered::new();

            let (recv, send) = socket.split();
                
            // Deserialize frames
            let mut requests = SymmetricallyFramed::new(
                FramedRead::new(recv, LengthDelimitedCodec::new()),
                SymmetricalJson::<Request>::default(),
            );

            let responses = SymmetricallyFramed::new(
                FramedWrite::new(send, LengthDelimitedCodec::new()),
                SymmetricalJson::<Response>::default(),
            );

            let (mut tx, rx) = futures::channel::mpsc::unbounded::<Response>();
            
            let mut multiplex_encode_task = rx.map(|message| Ok(message)).forward(responses);

            loop {
                tokio::select! {
                    Some((id, exit_result)) = processes.next() => {
                        process_tx.remove(&id);
                        let exit_result : io::Result<std::process::ExitStatus> = exit_result;
                        // todo, exit result is not necessarily ok
                        if let Ok(exit_result) = exit_result {
                            let termination = if exit_result.success() {
                                process::Response::Terminated(true)
                            }
                            else {
                                process::Response::Terminated(false)
                            };
                            let response = Response::Process(id, termination);
                            if let Err(error) = tx.send(response).await {
                                log::error!("Failed to send terminate message to client: {}", error);
                            }
                        }
                        else {
                            log::error!("process failed to exit");
                        }
                    },
                    result = &mut multiplex_encode_task => {
                        log::warn!("multiplex_encode_task: {:?}", result);
                    },
                    Ok(Some(message)) = requests.try_next() => {
                        let response = match message {
                            Request::Ping => {
                                log::info!("[{}] received ping", peer_addr);
                                Response::Ok
                            },
                            Request::Upload(upload) => {
                                let file_path = upload.path.join(upload.filename);
                                log::info!("[{}] uploaded {}", peer_addr, file_path.to_string_lossy());
                                let contents = upload.contents;
                                tokio::fs::create_dir_all(&upload.path)
                                    .and_then(|_| tokio::fs::write(&file_path, &contents)).await
                                    .into()
                            },
                            Request::Process(id, request) => {
                                log::info!("[{}] request to process {}, {:?}", peer_addr, id, request);
                                match process_tx.get_mut(&id) {
                                    Some(process_tx) => {
                                        match process_tx.send(request).await {
                                            Ok(_) => Response::Ok,
                                            Err(_) => Response::Error("Could not communicate with process".to_owned())
                                        }
                                        // send back ok for now, alternatively another pipe would have to be constructed
                                        // and added to map to allow two way communciation. Alternatively, we could
                                        // just not send anything back, and let the process respond
                                    },
                                    None => Response::Error("No such process".to_owned())
                                }
                            }
                            Request::Run(run) => {
                                let mut process = tokio::process::Command::new(run.target)
                                    .current_dir(run.working_dir)
                                    .args(run.args)
                                    .stdout(std::process::Stdio::piped())
                                    .stderr(std::process::Stdio::piped())
                                    .stdin(std::process::Stdio::piped())
                                    .spawn()
                                    .expect("failed to start process");

                                if let Some(id) = process.id() {
                                    /* control channel */
                                    let (proc_tx, mut proc_rx) = futures::channel::mpsc::unbounded::<process::Request>();
                                    /* stdout */
                                    let stdout = process.stdout.take().expect("could not get pipe for stdout");
                                    let stdout_stream = FramedRead::new(stdout, ProcessCodec::new(id, process::Source::Stdout));
                                    let mut stdout_forward = stdout_stream.forward(
                                        tx.clone().sink_map_err(|inner| {
                                            io::Error::new(io::ErrorKind::BrokenPipe, inner)
                                        })
                                    ).fuse();
                                    /* stderr */
                                    let stderr = process.stderr.take().expect("could not get pipe for stderr");
                                    let stderr_stream = FramedRead::new(stderr, ProcessCodec::new(id, process::Source::Stderr));
                                    let mut stderr_forward = stderr_stream.forward(
                                        tx.clone().sink_map_err(|inner| {
                                            io::Error::new(io::ErrorKind::BrokenPipe, inner)
                                        })
                                    ).fuse();
                                    /* stdin */
                                    let mut stdin = process.stdin.take().expect("could not get pipe for stdin");
                                    /* process loop */
                                    processes.push(async move {
                                        let exit_result : io::Result<std::process::ExitStatus> = loop {
                                            tokio::select! {
                                                Some(message) = proc_rx.next() => match message {
                                                    process::Request::Write(data) => {
                                                        if let Err(error) = stdin.write_all(&data).await {
                                                            log::error!("[{}] failed to write to stdin: {}", peer_addr, error);
                                                        }
                                                    },
                                                    process::Request::Kill => { 
                                                        if let Err(error) = process.start_kill() {
                                                            log::error!("[{}] failed to start kill: {}", peer_addr, error);
                                                        }
                                                    }
                                                },
                                                _ = &mut stdout_forward => {},
                                                _ = &mut stderr_forward => {},
                                                // note take out stdin!
                                                exit_result = process.wait() => {
                                                    if stdout_forward.is_terminated() &&
                                                       stderr_forward.is_terminated() {
                                                        break exit_result;
                                                    }
                                                },
                                            }
                                        };
                                        (id, exit_result)
                                    });
                                    /* insert the tx end of the control channel into a map*/
                                    process_tx.insert(id, proc_tx);

                                    Response::Process(id, process::Response::Started)
                                }
                                else {
                                    Response::Error("Something went wrong".to_owned())
                                }
                            },
                        };

                        if let Err(error) = tx.send(response).await {
                            eprintln!("Error = {}", error);
                        }
                    }
                } // select
            }
        }); // tokio::spawn
    }
}
