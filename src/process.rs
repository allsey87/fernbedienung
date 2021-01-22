use serde::{Deserialize, Serialize};
use bytes::BytesMut;

#[derive(Clone, Copy, Debug, Serialize)]
pub enum Source {
    Stdout, Stderr
}

#[derive(Debug, Deserialize)]
pub enum Request {
    Write(Vec<u8>),
    Kill,
}

#[derive(Debug, Serialize)]
pub enum Response {
    Started,
    Terminated(bool),
    Output(Source, BytesMut),
}