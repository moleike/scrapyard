use std::{
    io::{Read, Write},
    net::{TcpStream, ToSocketAddrs},
};

use crate::{
    Error::{KeyNotFound, ServerError},
    messages::{
        self,
        messages::{ErrorCode, Reply, Response},
    },
};

pub struct Client {
    stream: TcpStream,
}

impl Client {
    pub fn connect<T: ToSocketAddrs>(addr: T) -> crate::Result<Self> {
        Ok(Self {
            stream: TcpStream::connect(addr)?,
        })
    }

    pub fn get(&mut self, key: &str) -> crate::Result<String> {
        let req = messages::serialize_request_get(key);

        self.stream.write_all(&req.as_bytes())?;

        let buf = messages::read::<TcpStream, Response>(&mut self.stream)?;
        let res = buf.get_root()?;

        match res.reply_type() {
            Reply::GetValue => {
                let get_val_table = res.reply_as_get_value().ok_or(ServerError)?;

                let val = get_val_table.value();
                Some(val.unwrap_or("").to_string()).ok_or(ServerError)
            }
            Reply::Failure => {
                let fail = res.reply_as_failure().ok_or(ServerError)?;
                match fail.code() {
                    ErrorCode::NotFound => Err(KeyNotFound),
                    _ => Err(ServerError),
                }
            }
            _ => Err(ServerError),
        }
    }

    pub fn set(&mut self, key: &str, value: &str) -> crate::Result<()> {
        let req = messages::serialize_request_set(key, value);

        self.stream.write_all(&req.as_bytes())?;

        let buf = messages::read::<TcpStream, Response>(&mut self.stream)?;
        let res = buf.get_root()?;

        match res.reply_type() {
            Reply::Success => Ok(()),
            _ => Err(ServerError),
        }
    }

    pub fn delete(&mut self, key: &str) -> crate::Result<()> {
        let req = messages::serialize_request_delete(key);

        self.stream.write_all(&req.as_bytes())?;

        let buf = messages::read::<TcpStream, Response>(&mut self.stream)?;
        let res = buf.get_root()?;

        match res.reply_type() {
            Reply::Success => Ok(()),
            Reply::Failure => {
                let fail = res.reply_as_failure().ok_or(ServerError)?;
                match fail.code() {
                    ErrorCode::NotFound => Err(KeyNotFound),
                    _ => Err(ServerError),
                }
            }
            _ => Err(ServerError),
        }
    }
}
