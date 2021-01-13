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

#[derive(Clone, Copy, Debug, Serialize)]
enum Source {
    StandardOutput,
    StandardError,
}

#[derive(Debug, Serialize)]
enum Response {
    Ok,
    Error(String),
    Output(u32, Source, BytesMut),
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
            let response = Response::Output(self.0, self.1, buf.split_to(length));
            Ok(Some(response))
        } else {
            Ok(None)
        }
    }
}


// Since output on stdout, stderr is async and can happen at anytime, we probably need a mpsc channel
// for writing back to the client

// is it possible to wrap up the ChildStdout, a BufMut, the code to split etc into a async functor that returns bytes?
// This future would never complete and would require channels to get data in and out, but that could be ok?

#[tokio::main]
pub async fn main() {
    // Bind a server socket
    let listener = TcpListener::bind("127.0.0.1:17653").await.unwrap();

    println!("listening on {:?}", listener.local_addr());

    loop {
        let (mut socket, _peer_addr) = listener.accept().await.unwrap();

        tokio::spawn(async move {
            // by having this map only in the scope of this task, it is not possible
            // for different clients to access each others processes in general this
            // adds security, but would prevent regaining control over processes after
            // disconnects
            // TODO security isn't important for this application, move this hashmap out
            // of the client and protect with RwLock?
            let mut processes : HashMap<u32, tokio::process::Child> = HashMap::new();

            /*
            Two approaches:
            HashMap<id, child>
                - easy access from the request loops, processes.get_mut().kill() etc
                - stdout/stderr? need to be made into streams (requires move or mutable borrow, conflicts with)
                - FramedRead::new(my_async_read, BytesCodec::new());

            FuturesUnordered<[...async...]>
                - access from the request loop is a bit more complicated
                - in the main loop, we would have HashMap<id, tx_pipe>
                    - either a hashmap+pipe for each operation, e.g., write,kill
                    - single hashmap+pipe per process that forwards the tx related requests?
                        - Write(u32, Vec<u8>), Kill(u32): note the u32 could be dropped/no longer makes sense
                        - A new type would be needed e.g., ProcessRequest
                - inside the [..async..] block we would handle all process related actions, essentially
                  there would be a big select! statement in a loop that waits for a ProcessRequest from the rx end
                  of the pipe
                - how to push data from stderr and stdout back to client?
                - the problem here is that this [...async...] should not resolve until the process has terminated,
                  since yield is experimental, we should use pipes to get data out of the async closure
                - the rx end of these pipes need to be near the response loop so that we can generate responses

            */

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
            //tokio::sync::mpsc::unbounded_channel::<Response>();

            


            // everything that needs to go back to the server goes through the above channel
            // the following tasks need to be concurrently awaited on
            // 1. The rx end of the channel needs to drain into responses
            // 2. We need to respond to recevived messages from requests
            // 3. We need to continously read from stdout, stderr FOR EACH PROCESS, encapsulate
            // that data as a response and write it to responses
            // 4* It may be possible to use a FuturesUnordered collection to poll all the
            // running processes

            // the futures in this collection are waiting on stdout and stderr to produce data
            // this collection needs to be polled at the same level as requests. The result of
            // both a request and a stdout/stderr event is returning a response
            
            
            let result = tokio::join!(rx.map(|msg| Ok(msg)).forward(responses), async {
                let stdout_streams = FuturesUnordered::new();
                let stderr_streams = FuturesUnordered::new();

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
                                
                                // one future per process?
                                // each process contains a select statement on read/write/kill/wait
                                // start each process on its own tokio thread, use channels to R/W
                                
                                //let stderr = process.stderr.as_mut().unwrap();
                                //let stdin = process.stdin.as_mut().unwrap();

                                // this bytescodec is insufficient since it doesn't include the process id
                                // etc, it would be better if the codec generated a Response or ProcessResp directly
                                
                                let stdout = process.stdout.take().unwrap();
                                let stderr = process.stderr.take().unwrap();
                                let stdout_stream = FramedRead::new(stdout, ProcessCodec::new(id, Source::StandardOutput));
                                let stderr_stream = FramedRead::new(stderr, ProcessCodec::new(id, Source::StandardError));
                                //FramedRead::new(stderr, BytesCodec::new());
                                // two futuresunordered may be necessary, one for the stderr, one for stdout
                                // then tokio::select! over the two future collections and the received messages

                                // tx is UnboundedSender<Response> of the futures::channel::mpsc::unbounded::<Response>();
                                // changed to the futures channel since it implements sink
                                // items from stdout stream are Option<Result<Response, Error>>
                                // we need to map, convert this, first question is what is forward expecting?
                                // Forward is expecting something that implements TryStream
                                // TryStream has a blanket implementation over any Stream whose Item = Result<T,E>
                                // FramedRead implements Stream, perhaps the problem here is the Option?

                                //let tx = tx.sink_map_err(|e| e);
                                let stdout_forward = stdout_stream.forward(
                                    tx.clone().sink_map_err(|inner| {
                                        io::Error::new(io::ErrorKind::BrokenPipe, inner)
                                    })
                                );
                                stdout_streams.push(stdout_forward);

                                let stderr_forward = stderr_stream.forward(
                                    tx.clone().sink_map_err(|inner| {
                                        io::Error::new(io::ErrorKind::BrokenPipe, inner)
                                    })
                                );
                                stderr_streams.push(stderr_forward);


                                // place the child into the map so that we can kill it/write to stdin
                                processes.insert(id, process);

                                
                            }
                            Response::Ok
                        },
                    };

                    if let Err(error) = tx.send(response).await {
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
