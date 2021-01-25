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
use uuid::Uuid;

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
enum RequestKind {
    Ping,
    Upload(Upload),
    /* it feels more and more like these two can be merged */
    Run(Run),
    Process(Uuid, process::Request),
}

#[derive(Debug, Deserialize)]
struct Request(Uuid, RequestKind);

#[derive(Debug, Serialize)]
enum ResponseKind {
    Ok,
    Error(String),
    Process(Uuid, process::Response),
}

#[derive(Debug, Serialize)]
struct Response(Uuid, ResponseKind);


impl<T, E: std::fmt::Display> std::convert::From<(uuid::Uuid, Result<T, E>)> for Response {
    fn from((uuid, result): (Uuid, Result<T, E>)) -> Self {
        Response(uuid, match result {
            Ok(_) => ResponseKind::Ok,
            Err(error) => ResponseKind::Error(error.to_string())
        })
    }
}

struct ProcessCodec(Uuid, process::Source);

impl ProcessCodec {
    fn new(id: Uuid, source: process::Source) -> ProcessCodec {
        ProcessCodec(id, source)
    }
}

impl Decoder for ProcessCodec {
    type Item = Response;
    type Error = io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Response>, io::Error> {
        if !buf.is_empty() {
            let length = buf.len();
            let response = Response(self.0, ResponseKind::Process(self.0, process::Response::Output(self.1, buf.split_to(length))));
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
            let mut process_tx : HashMap<Uuid, UnboundedSender<process::Request>> = HashMap::new();
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
                    Some((uuid, exit_result)) = processes.next() => {
                        process_tx.remove(&uuid);
                        let exit_result : io::Result<std::process::ExitStatus> = exit_result;
                        // todo, exit result is not necessarily ok
                        if let Ok(exit_result) = exit_result {
                            let termination = if exit_result.success() {
                                process::Response::Terminated(true)
                            }
                            else {
                                process::Response::Terminated(false)
                            };
                            let response = Response(uuid, ResponseKind::Process(uuid, termination));
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
                    // is using next instead of try_next better here?
                    Ok(Some(Request(uuid, request))) = requests.try_next() => {
                        let response = match request {
                            RequestKind::Ping => {
                                log::info!("[{}] received ping", peer_addr);
                                Response(uuid, ResponseKind::Ok)
                            },
                            RequestKind::Upload(upload) => {
                                let file_path = upload.path.join(upload.filename);
                                log::info!("[{}] uploaded {}", peer_addr, file_path.to_string_lossy());
                                let contents = upload.contents;
                                let result = tokio::fs::create_dir_all(&upload.path)
                                    .and_then(|_| tokio::fs::write(&file_path, &contents)).await;
                                Response(uuid, match result {
                                    Ok(_) => ResponseKind::Ok,
                                    Err(error) => ResponseKind::Error(error.to_string())
                                })
                            },
                            RequestKind::Process(id, request) => {
                                log::info!("[{}] request to process {}, {:?}", peer_addr, id, request);
                                Response(uuid, match process_tx.get_mut(&id) {
                                    Some(process_tx) => match process_tx.send(request).await {
                                        Ok(_) => ResponseKind::Ok,
                                        Err(_) => ResponseKind::Error("Could not communicate with process".to_owned())
                                    },
                                    None => ResponseKind::Error("No such process".to_owned())
                                })
                            }
                            RequestKind::Run(run) => {
                                let mut process = tokio::process::Command::new(run.target)
                                    .current_dir(run.working_dir)
                                    .args(run.args)
                                    .stdout(std::process::Stdio::piped())
                                    .stderr(std::process::Stdio::piped())
                                    .stdin(std::process::Stdio::piped())
                                    .spawn()
                                    .expect("failed to start process");

                                /* control channel */
                                let (proc_tx, mut proc_rx) = futures::channel::mpsc::unbounded::<process::Request>();
                                /* stdout */
                                let stdout = process.stdout.take().expect("could not get pipe for stdout");
                                let stdout_stream = FramedRead::new(stdout, ProcessCodec::new(uuid, process::Source::Stdout));
                                let mut stdout_forward = stdout_stream.forward(
                                    tx.clone().sink_map_err(|inner| {
                                        io::Error::new(io::ErrorKind::BrokenPipe, inner)
                                    })
                                ).fuse();
                                /* stderr */
                                let stderr = process.stderr.take().expect("could not get pipe for stderr");
                                let stderr_stream = FramedRead::new(stderr, ProcessCodec::new(uuid, process::Source::Stderr));
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
                                    (uuid, exit_result)
                                });
                                /* insert the tx end of the control channel into a map*/
                                process_tx.insert(uuid, proc_tx);

                                Response(uuid, ResponseKind::Process(uuid, process::Response::Started))
                            }
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
