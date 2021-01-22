#![warn(rust_2018_idioms)]

use std::path::PathBuf;
use std::collections::HashMap;
use std::io;

use serde::{Deserialize, Serialize};

use futures::{prelude::*, stream::FuturesUnordered, channel::mpsc::{UnboundedSender, UnboundedReceiver}};

use tokio::net::TcpListener;
use tokio::io::{AsyncWriteExt};
use tokio_serde::{SymmetricallyFramed, formats::SymmetricalJson};
use tokio_util::codec::{Decoder, FramedRead, FramedWrite, LengthDelimitedCodec};

use bytes::{BytesMut};

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
    Process(u32, ProcessReq),
}

#[derive(Debug, Deserialize)]
enum ProcessReq {
    Write(Vec<u8>),
    Kill,
}

#[derive(Debug, Serialize)]
enum Response {
    Ok,
    Error(String),
    Process(u32, ProcessResp),
}

#[derive(Debug, Serialize)]
enum ProcessResp {
    Started,
    Terminated(usize),
    Output(Source, BytesMut),
}

#[derive(Clone, Copy, Debug, Serialize)]
enum Source {
    Stdout, Stderr
}


impl<T, E: std::fmt::Display> std::convert::From<Result<T, E>> for Response {
    fn from(result: Result<T, E>) -> Self {
        match result {
            Ok(_) => Response::Ok,
            Err(error) => Response::Error(error.to_string())
        }
    }
}

struct ProcessCodec(u32, Source);

impl ProcessCodec {
    fn new(pid: u32, source: Source) -> ProcessCodec {
        ProcessCodec(pid, source)
    }
}

impl Decoder for ProcessCodec {
    type Item = Response;
    type Error = io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Response>, io::Error> {
        if !buf.is_empty() {
            let length = buf.len();
            let response = Response::Process(self.0, ProcessResp::Output(self.1, buf.split_to(length)));
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
            let mut process_tx : HashMap<u32, UnboundedSender<ProcessReq>> = HashMap::new();
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
                    Some((id, _exit_result)) = processes.next() => {
                        process_tx.remove(&id);
                        // todo, exit result is not necessarily ok
                        let response = Response::Process(id, ProcessResp::Terminated(0));
                        if let Err(error) = tx.send(response).await {
                            log::error!("Failed to send terminate message to client: {}", error);
                        }
                    },
                    result = &mut multiplex_encode_task => {
                        log::warn!("multiplex_encode_task: {:?}", result);
                    },
                    Ok(Some(message)) = requests.try_next() => {
                        let response = match message {
                            Request::Ping => {
                                log::info!("[{}] Received ping", peer_addr);
                                Response::Ok
                            },
                            Request::Upload(upload) => {
                                let file_path = upload.path.join(upload.filename);
                                log::info!("[{}] Uploaded {}", peer_addr, file_path.to_string_lossy());
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
                                    // TODO: don't unwrap here, return the error if the process did not
                                    // start
                                    .unwrap();

                                if let Some(id) = process.id() {
                                    let stdout = process.stdout.take().unwrap();
                                    let stdout_stream = FramedRead::new(stdout, ProcessCodec::new(id, Source::Stdout));
                                    let stdout_forward = stdout_stream.forward(
                                        tx.clone().sink_map_err(|inner| {
                                            io::Error::new(io::ErrorKind::BrokenPipe, inner)
                                        })
                                    ).fuse();
                                    let stderr = process.stderr.take().unwrap();
                                    let stderr_stream = FramedRead::new(stderr, ProcessCodec::new(id, Source::Stderr));
                                    let stderr_forward = stderr_stream.forward(
                                        tx.clone().sink_map_err(|inner| {
                                            io::Error::new(io::ErrorKind::BrokenPipe, inner)
                                        })
                                    ).fuse();
                                    let (proc_tx, mut proc_rx) = futures::channel::mpsc::unbounded::<ProcessReq>();

                                    processes.push(async move {
                                        tokio::pin!(stdout_forward);
                                        tokio::pin!(stderr_forward);
                                        let exit_result = loop {
                                            tokio::select! {
                                                Some(message) = proc_rx.next() => match message {
                                                    // todo write to stdin
                                                    ProcessReq::Write(_data) => {},
                                                    ProcessReq::Kill => { 
                                                        process.kill().await.expect("kill failed");
                                                    }
                                                },
                                                _ = &mut stdout_forward => {
                                                    // problem: stdout does not always complete before
                                                    // wait does
                                                    log::info!("stdout forward completed")
                                                },
                                                _ = &mut stderr_forward => {
                                                    log::info!("stderr forward completed")
                                                },
                                                // note take out stdin!
                                                exit_result = process.wait() => break exit_result,
                                            }
                                        };
                                        (id, exit_result)
                                    });
                                    process_tx.insert(id, proc_tx);

                                    Response::Process(id, ProcessResp::Started)
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
