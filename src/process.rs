use bytes::BytesMut;
use futures::{SinkExt, StreamExt, channel::mpsc::UnboundedSender, future::FutureExt};
use serde::{Deserialize, Serialize};
use std::{future::Future, io, path::PathBuf, pin::Pin, process::ExitStatus};
use tokio_util::codec::{FramedRead, Decoder};
use tokio::process::ChildStdin;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Serialize)]
pub enum Source {
    Stdout, Stderr
}

#[derive(Debug, Deserialize)]
pub struct Run {
    target: PathBuf,
    working_dir: PathBuf,
    args: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub enum Request {
    Run(Run),
    Write(Vec<u8>),
    Signal(u32),
}

#[derive(Debug, Serialize)]
pub enum Response {
    Started,
    Terminated(bool),
    Output(Source, BytesMut),
}

struct Codec(Uuid, Source);

impl Codec {
    fn new(id: Uuid, source: Source) -> Codec {
        Codec(id, source)
    }
}

impl Decoder for Codec {
    type Item = crate::Response;
    type Error = io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<crate::Response>, io::Error> {
        if !buf.is_empty() {
            let length = buf.len();
            let response = crate::Response(Some(self.0), crate::ResponseKind::Process(Response::Output(self.1, buf.split_to(length))));
            Ok(Some(response))
        } else {
            Ok(None)
        }
    }
}

type Output = (Uuid, io::Result<ExitStatus>);

pub struct Process {
    pub uuid: Uuid,
    pub pid: Option<u32>,
    pub stdin: Option<ChildStdin>,
    handle: Pin<Box<dyn Future<Output = Output> + Send + Sync>>,
}

impl Future for Process {
    type Output = Output;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        self.handle.poll_unpin(cx)
    }
}

impl Process {
    pub fn new(tx: UnboundedSender<crate::Response>, uuid: Uuid, run: Run) -> Self {
        let mut process = tokio::process::Command::new(run.target)
            .current_dir(run.working_dir)
            .args(run.args)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .stdin(std::process::Stdio::piped())
            .spawn()
            .expect("failed to start process");
        /* stdout */
        let stdout = process.stdout.take().expect("could not get pipe for stdout");
        let stdout_stream = FramedRead::new(stdout, Codec::new(uuid, Source::Stdout));
        let stdout_forward = stdout_stream.forward(
            tx.clone().sink_map_err(|inner| {
                io::Error::new(io::ErrorKind::BrokenPipe, inner)
            })
        ).fuse();
        /* stderr */
        let stderr = process.stderr.take().expect("could not get pipe for stderr");
        let stderr_stream = FramedRead::new(stderr, Codec::new(uuid, Source::Stderr));
        let stderr_forward = stderr_stream.forward(
            tx.clone().sink_map_err(|inner| {
                io::Error::new(io::ErrorKind::BrokenPipe, inner)
            })
        ).fuse();
        /* stdin */
        let stdin = process.stdin.take();
        let pid = process.id().clone();
        let handle = async move {
            let (_, _, wait_result) = 
                tokio::join!(stdout_forward, stderr_forward, process.wait());
            (uuid, wait_result)
        };
        Process { uuid, pid, stdin, handle: Box::pin(handle) }
    }
}
