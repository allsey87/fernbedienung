#![warn(rust_2018_idioms)]

use futures::{prelude::*, stream::FuturesUnordered};
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, path::PathBuf};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use tokio::{io::AsyncWriteExt, net::TcpListener};
use tokio_serde::{SymmetricallyFramed, formats::SymmetricalJson};
use uuid::Uuid;

mod process;

#[derive(Debug, Deserialize)]
pub struct Upload {
    filename: PathBuf,
    path: PathBuf,
    contents: Vec<u8>,
}

#[derive(Debug, Deserialize)]
pub enum RequestKind {
    Ping,
    Upload(Upload),
    Process(process::Request),
}

#[derive(Debug, Deserialize)]
pub struct Request(Uuid, RequestKind);

#[derive(Debug, Serialize)]
pub enum ResponseKind {
    Ok,
    Error(String),
    Process(process::Response),
}

#[derive(Debug, Serialize)]
pub struct Response(Option<Uuid>, ResponseKind);

#[tokio::main]
pub async fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("fernbedienung=info")).init();

    // Bind a server socket
    let listener = TcpListener::bind("0.0.0.0:17653").await.unwrap();

    log::info!("listening on {:?}", listener.local_addr().unwrap());

    loop {
        let (mut socket, peer_addr) = listener.accept().await.unwrap();
        log::info!("[{}] connected", peer_addr);
        tokio::spawn(async move {
            let mut processes = FuturesUnordered::new();
            /* client communication */
            let (recv, send) = socket.split();
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
            /* main client loop */
            loop {
                tokio::select! {
                    Some((uuid, exit_result)) = processes.next() => {
                        let exit_result : std::io::Result<std::process::ExitStatus> = exit_result;
                        let termination = match exit_result {
                            Ok(status) => process::Response::Terminated(status.success()),
                            Err(_) => process::Response::Terminated(false)
                        };
                        let response = Response(Some(uuid), ResponseKind::Process(termination));
                        if let Err(error) = tx.send(response).await {
                            log::error!("failed to send terminate message to client: {}", error);
                        }
                    },
                    result = &mut multiplex_encode_task => {
                        log::warn!("multiplex encode task completed: {:?}", result);
                    },
                    request = requests.next() => match request {                  
                        Some(request) => {
                            let response = handle_client_request(request, &peer_addr, &tx, &mut processes).await;
                            if let Err(error) = tx.send(response).await {
                                log::error!("{}", error.to_string());
                            }
                        },
                        /* at this point it is probably the case that the client has disconnected */
                        None => break
                    }
                }
            }
            log::info!("[{}] disconnected", peer_addr);
        });
    }
}

async fn handle_client_request(request: std::io::Result<Request>, 
                               peer_addr: &SocketAddr,
                               tx: &futures::channel::mpsc::UnboundedSender<Response>,
                               processes: &mut FuturesUnordered<process::Process>) -> Response {
    match request {
        Ok(Request(uuid, request)) => match request {
            RequestKind::Ping => {
                log::info!("[{}] ping", peer_addr);
                Response(Some(uuid), ResponseKind::Ok)
            },
            RequestKind::Upload(upload) => {
                let file_path = upload.path.join(upload.filename);
                log::info!("[{}] uploading {}", peer_addr, file_path.to_string_lossy());
                let contents = upload.contents;
                let result = tokio::fs::create_dir_all(&upload.path)
                    .and_then(|_| tokio::fs::write(&file_path, &contents)).await;
                Response(Some(uuid), match result {
                    Ok(_) => ResponseKind::Ok,
                    Err(error) => ResponseKind::Error(error.to_string())
                })
            },
            RequestKind::Process(request) => Response(Some(uuid), match request {
                process::Request::Run(run) => {
                    log::info!("[{}] running {:?}", peer_addr, run);
                    let process = process::Process::new(tx.clone(), uuid, run);
                    processes.push(process);
                    ResponseKind::Process(process::Response::Started)
                },
                process::Request::StandardInput(data) => {
                    log::info!("[{}] writing {:?}", peer_addr, data);
                    let stdin = processes
                        .iter_mut()
                        .find(|process| process.uuid == uuid)
                        .and_then(|process| process.stdin.take());
                    match stdin {
                        Some(mut input) => {
                            let response = match input.write_all(data.as_ref()).await {
                                Ok(_) => ResponseKind::Ok,
                                Err(error) => ResponseKind::Error(error.to_string())
                            };
                            /* try to put the standard input back */
                            processes
                                .iter_mut()
                                .find(|process| process.uuid == uuid)
                                .map(|process| process.stdin.get_or_insert(input));
                            /* return the response */
                            response
                        },
                        None => ResponseKind::Error("Standard input unavailable".to_owned())
                    }
                },
                process::Request::Signal(number) => {
                    log::info!("[{}] signaling {:?}", peer_addr, number);
                    let pid = processes
                        .iter()
                        .find(|process| process.uuid == uuid)
                        .and_then(|process| process.pid);
                    match pid {
                        Some(pid) => {
                            let process = tokio::process::Command::new("kill")
                                .arg(format!("-{}", number))    
                                .arg(format!("{}", pid))
                                .output();
                            match process.await {
                                Ok(std::process::Output {status, ..}) => match status.code() {
                                    Some(0) => ResponseKind::Ok,
                                    Some(code) => ResponseKind::Error(format!("signal terminated with {}", code)),
                                    None => ResponseKind::Error(format!("signal terminated without code"))
                                },
                                Err(error) => ResponseKind::Error(error.to_string()),
                            }
                        },
                        None => ResponseKind::Error("Could not find process".to_owned())
                    }
                }
            })
        },
        Err(error) => Response(None, ResponseKind::Error(error.to_string()))
    }
}