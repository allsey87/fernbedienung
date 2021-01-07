#![warn(rust_2018_idioms)]

use std::path::PathBuf;
use std::collections::HashMap;
use std::io;

use serde::{Deserialize, Serialize};

use futures::{stream::FuturesUnordered, prelude::*};

use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_serde::formats::SymmetricalJson;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

use bytes::{BufMut, BytesMut};

// Important: tokio::stream::Stream is a reexport of futures::stream::Stream, however, both libraries have different versions of
// StreamExt

// add as systemd process
// https://hub.mender.io/t/how-to-create-your-first-recipe-and-enable-auto-start-using-systemd/1195

/*
#[derive(thiserror::Error, Debug)]
enum Error {
    #[error(transparent)]
    JsonError(#[from] serde_json::Error),

    #[error(transparent)]
    IoError(#[from] std::io::Error),
}

type Result<T> = std::result::Result<T, Error>;
*/

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
    StdErr(u32, Vec<u8>),
    StdOut(u32, Vec<u8>),
}

impl<T, E: std::fmt::Display> std::convert::From<Result<T, E>> for Response {
    fn from(result: Result<T, E>) -> Self {
        match result {
            Ok(_) => Response::Ok,
            Err(error) => Response::Error(error.to_string())
        }
    }
}

// Since output on stdout, stderr is async and can happen at anytime, we probably need a mpsc channel
// for writing back to the client

// is it possible to wrap up the ChildStdout, a BufMut, the code to split etc into a async functor that returns bytes?
// This future would never complete and would require channels to get data in and out, but that could be ok?
enum ProcessResp {
    Status(usize), // PID
    StdErr,
    StdOut,
}


#[tokio::main]
pub async fn main() {
    // Bind a server socket
    let mut listener = TcpListener::bind("127.0.0.1:17653").await.unwrap();

    println!("listening on {:?}", listener.local_addr());

    
    while let Some(mut socket) = listener.try_next().await.unwrap() {
        
        tokio::spawn(async move {
            // by having this map only in the scope of this task, it is not possible
            // for different clients to access each others processes in general this
            // adds security, but would prevent regaining control over processes after
            // disconnects
            let mut processes : HashMap<u32, tokio::process::Child> = HashMap::new();
            
            let (recv, send) = socket.split();
                
            // Deserialize frames
            let mut requests = tokio_serde::SymmetricallyFramed::new(
                FramedRead::new(recv, LengthDelimitedCodec::new()),
                SymmetricalJson::<Request>::default(),
            );

            let mut responses = tokio_serde::SymmetricallyFramed::new(
                FramedWrite::new(send, LengthDelimitedCodec::new()),
                SymmetricalJson::<Response>::default(),
            );

            let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Response>();

            // everything that needs to go back to the server goes through the above channel
            // the following tasks need to be concurrently awaited on
            // 1. The rx end of the channel needs to drain into responses
            // 2. We need to respond to recevived messages from requests
            // 3. We need to continously read from stdout, stderr FOR EACH PROCESS, encapsulate
            // that data as a response and write it to responses
            // 4* It may be possible to use a FuturesUnordered collection to poll all the
            // running processes

            let tasks = FuturesUnordered::new();
            
            let result = tokio::join!(rx.map(|msg| Ok(msg)).forward(responses), async {
                while let Some(msg) = requests.try_next().await.unwrap() {
                    let response = match msg {
                        Request::Ping => Response::Ok,
                        Request::Upload(upload) => {
                            let file_path = upload.path.join(upload.filename);
                            let contents = upload.contents;
                            tokio::fs::create_dir_all(&upload.path)
                                .and_then(|_| tokio::fs::write(&file_path, &contents)).await
                                .into()
                        },
                        Request::Write(id, data) => {
                            let f = processes.get_mut(&id)
                                .ok_or(io::Error::new(io::ErrorKind::NotFound, "No such process"))
                                .and_then(|process| process.stdin
                                    .as_mut()
                                    .ok_or(io::Error::new(io::ErrorKind::BrokenPipe, "Standard input unavailable")))
                                .map(|p| p.write_all(&data));
                            
                            Response::Ok
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
                                .unwrap();

                            if let Some(id) = process.id() {
                                
                                // one future per process?
                                // each process contains a select statement on read/write/kill/wait
                                // start each process on its own tokio thread, use channels to R/W
                                
                                let tx_clone = tx.clone();

                                // current approach is to move stdout and stderr away from their process and put them
                                // in this closure, so that the process can be put into a vector which can then be
                                // killed, written to etc
                                
                                let (stdin_tx, mut stdin_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();

                                // alternatively, we can move the process itself inside of this async block, and use
                                // channels to handle write/kill
                                tasks.push(async move {
                                    let stdout = process.stdout.as_mut().unwrap();
                                    let stderr = process.stderr.as_mut().unwrap();
                                    let stdin = process.stdin.as_mut().unwrap();

                                    let test = stdin_rx.map(|data| Ok(data.as_ref())).forward(stdin);

                                    let mut stdout_buffer = BytesMut::new();
                                    let mut stderr_buffer = BytesMut::new();
                                    loop {
                                        tokio::select!{
                                            Ok(_) = stdout.read_buf(&mut stdout_buffer) => {
                                                // if tx_clone implements sink, 
                                                let response = Response::StdOut(id, stdout_buffer.split().to_vec());
                                                if let Err(_) = tx_clone.send(response) {
                                                    /* this error implies that the rx end was closed or dropped */
                                                    /* exit this loop so that this future resolves */
                                                    break;
                                                }
                                            },
                                            Ok(_) = stderr.read_buf(&mut stderr_buffer) => {
                                                let response = Response::StdErr(id, stderr_buffer.split().to_vec());
                                                if let Err(_) = tx_clone.send(response) {
                                                    /* this error implies that the rx end was closed or dropped */
                                                    /* exit this loop so that this future resolves */
                                                    break;
                                                }
                                            }
                                            
                                        }
                                    }
                                });

                                // two futuresunordered may be necessary, one for the stderr, one for stdout
                            }
                            Response::Ok
                        },
                    };

                    if let Err(error) = tx.send(response) {
                        eprintln!("Error = {}", error);
                    }
                }
            });
        });
    }
}

/*

allsey87 Today at 11:45
Is it possible to convert tokio::process::ChildStdin into something that implements Sink so that it can be forwarded to from an unbounded channel?
Alice Ryhl Today at 11:52
Yes. FramedWrite in tokio_util
allsey87 Today at 11:53
Huh, that wasn't the answer I was expecting, I'll check it out, thanks!
Alice Ryhl Today at 11:58
@allsey87 you might want to check https://github.com/tokio-rs/tokio/pull/3283/files out
I recently wrote more documentation for that module, but it isn't out yet
allsey87Today at 12:14
I just noticed that I didn't mention that by unbounded channel is sending/receiving Vec<u8>, is it still necessary to use FramedWrite?
Alice RyhlToday at 12:32
Yes, but you can use the inbuilt codec https://docs.rs/tokio-util/0.5.1/tokio_util/codec/struct.BytesCodec.html in that case
actually
@allsey87 rather than converting the ChildStdin to a Sink, in this case it's better to convert your Stream<Item = Vec<u8>> to an AsyncRead.
You can do this using the StreamReader utility
at that point you can use tokio::io::copy or tokio::io::copy_buf


    

    if let Some(id) = process.id() {
        processes.insert(id, process);

    }
    else {
        /* kill and return id? */
    }

    let f = async {
        let mut stdout_buffer = BytesMut::new();
        let mut stderr_buffer = BytesMut::new();

        loop {
            tokio::select! {
                Ok(_) = stdout.read_buf(&mut stdout_buffer) => {
                    eprintln!("{:?}", std::str::from_utf8(stdout_buffer.split().as_ref()));
                },
                Ok(_) = stderr.read_buf(&mut stderr_buffer) => {
                    eprintln!("{:?}", std::str::from_utf8(stderr_buffer.split().as_ref()));
                }

            }
        }
    };

                        
*/
