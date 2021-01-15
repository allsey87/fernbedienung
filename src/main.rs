#![warn(rust_2018_idioms)]

use std::path::PathBuf;
use std::collections::HashMap;
use std::io;

use serde::{Deserialize, Serialize};

use futures::{prelude::*, stream::FuturesUnordered};

use tokio::net::TcpListener;
use tokio::io::{AsyncWriteExt};
use tokio_serde::formats::SymmetricalJson;
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
    Write(u32, Vec<u8>),
    Kill(u32),
}

#[derive(Debug, Serialize)]
enum Response {
    Ok,
    Error(String),
    Process(u32, Process),
}

#[derive(Debug, Serialize)]
enum Process {
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
            let response = Response::Process(self.0, Process::Output(self.1, buf.split_to(length)));
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
            let mut processes : HashMap<u32, tokio::process::Child> = HashMap::new();

            let (recv, send) = socket.split();
                
            // Deserialize frames
            let mut requests = tokio_serde::SymmetricallyFramed::new(
                FramedRead::new(recv, LengthDelimitedCodec::new()),
                SymmetricalJson::<Request>::default(),
            );

            let responses = tokio_serde::SymmetricallyFramed::new(
                FramedWrite::new(send, LengthDelimitedCodec::new()),
                SymmetricalJson::<Response>::default(),
            );

            let (mut tx, rx) = futures::channel::mpsc::unbounded::<Response>();
            
            let mut multiplex_encode_task = rx.map(|message| Ok(message)).forward(responses);
            let mut forward_stdout_tasks = FuturesUnordered::new();
            let mut forward_stderr_tasks = FuturesUnordered::new();

            loop {
                tokio::select! {
                    Err(error) = forward_stderr_tasks.try_next() => {
                        log::error!("forward_stderr_tasks: {:?}", error);
                    },
                    Err(error) = forward_stdout_tasks.try_next() => {
                        log::error!("forward_stdout_tasks: {:?}", error);
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
                            Request::Write(id, data) => {
                                log::info!("[{}] Wrote to process {}", peer_addr, id);
                                let t = processes.get_mut(&id)
                                .ok_or(io::Error::new(io::ErrorKind::NotFound, "No such process"))
                                .and_then(|process| process.stdin
                                    .as_mut()
                                    .ok_or(io::Error::new(io::ErrorKind::BrokenPipe, "Standard input unavailable")));

                                match t {
                                    /* match against the result of getting the child's stdin */
                                    Ok(stdin) => stdin.write_all(&data).await.into(),
                                    Err(error) => Err::<(), io::Error>(error).into(),
                                }
                            },
                            Request::Kill(id) => {
                                processes.get_mut(&id)
                                    .ok_or(io::Error::new(io::ErrorKind::NotFound, "No such process"))
                                    .and_then(tokio::process::Child::start_kill)
                                    .into()
                            },
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
                                    // create a single async block here that handles all process IO?
                                    // channels for reading, writing, killing, waiting
                                    // something like this in main loop
                                    let tprocesses = FuturesUnordered::new();
                                    enum ProcessRequest {
                                        Write(Vec<u8>),
                                        Kill,
                                    }

                                    let (_, mut proc_rx) : (_, futures::channel::mpsc::UnboundedReceiver<ProcessRequest>)
                                    
                                        = futures::channel::mpsc::unbounded::<ProcessRequest>();
                                    
                                    // using a single async block here should make the instances compatible
                                    tprocesses.push(async move {
                                        match proc_rx.next().await {
                                            None => {}
                                            Some(message) => match message {
                                                ProcessRequest::Write(_data) => {},
                                                ProcessRequest::Kill => {}
                                            }

                                        }
                                        //tokio::select! proc_rx.next for write, kill

                                    });



                                    

                                    let stdout = process.stdout.take().unwrap();
                                    let stdout_stream = FramedRead::new(stdout, ProcessCodec::new(id, Source::Stdout));
                                    let stdout_forward = stdout_stream.forward(
                                        tx.clone().sink_map_err(|inner| {
                                            io::Error::new(io::ErrorKind::BrokenPipe, inner)
                                        })
                                    );
                                    forward_stdout_tasks.push(stdout_forward);

                                    let stderr = process.stderr.take().unwrap();
                                    let stderr_stream = FramedRead::new(stderr, ProcessCodec::new(id, Source::Stderr));
                                    let stderr_forward = stderr_stream.forward(
                                        tx.clone().sink_map_err(|inner| {
                                            io::Error::new(io::ErrorKind::BrokenPipe, inner)
                                        })
                                    );
                                    forward_stderr_tasks.push(stderr_forward);

                                    //let terminate = process.wait();


                                    // place the child into the map so that we can kill it/write to stdin
                                    processes.insert(id, process);

                                    Response::Process(id, Process::Started)
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
