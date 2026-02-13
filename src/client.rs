use std::{
    io::{Read, Write},
    net::{TcpStream, ToSocketAddrs},
};

use crate::{
    messages::{self, messages::{
        ErrorCode, Reply, Response
    }}, Error::{KeyNotFound, ServerError}
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
        let bytes = messages::serialize_request_get(key);

        self.stream.write_all(&bytes)?;

        let buf = Self::get_raw_response(&mut self.stream)?;

        let response = Self::get_response(&buf)?;

        match response.reply_type() {
            Reply::GetValue => {
                let get_val_table = response.reply_as_get_value().ok_or(ServerError)?;

                let val = get_val_table.value();
                Some(val.unwrap_or("").to_string()).ok_or(ServerError)
            }
            Reply::Failure => {
                let fail = response.reply_as_failure().ok_or(ServerError)?;
                match fail.code() {
                    ErrorCode::NotFound => Err(KeyNotFound),
                    _ => Err(ServerError),
                }
            }
            _ => Err(ServerError),
        }
    }

    pub fn set(&mut self, key: &str, value: &str) -> crate::Result<()> {
        let bytes = messages::serialize_request_set(key, value);

        self.stream.write_all(&bytes)?;

        let buf = Self::get_raw_response(&mut self.stream)?;

        let response = Self::get_response(&buf)?;

        match response.reply_type() {
            Reply::Success => Ok(()),
            _ => Err(ServerError),
        }
    }

    pub fn delete(&mut self, key: &str) -> crate::Result<()> {
        let bytes = messages::serialize_request_delete(key);

        self.stream.write_all(&bytes)?;

        let buf = Self::get_raw_response(&mut self.stream)?;

        let response = Self::get_response(&buf)?;

        match response.reply_type() {
            Reply::Success => Ok(()),
            Reply::Failure => {
                let fail = response.reply_as_failure().ok_or(ServerError)?;
                match fail.code() {
                    ErrorCode::NotFound => Err(KeyNotFound),
                    _ => Err(ServerError),
                }
            }
            _ => Err(ServerError),
        }
    }

    fn get_raw_response<R: Read>(input: &mut R) -> crate::Result<Vec<u8>> {
        let mut size_buf = [0u8; 4];
        input.read_exact(&mut size_buf)?;

        let size = u32::from_le_bytes(size_buf) as usize;

        let mut msg_buf = vec![0u8; size];
        input.read_exact(&mut msg_buf)?;

        let mut full_buffer = Vec::with_capacity(size + 4);
        full_buffer.extend_from_slice(&size_buf);
        full_buffer.extend_from_slice(&msg_buf);

        Ok(full_buffer)
    }

    fn get_response<'a>(bytes: &'a Vec<u8>) -> crate::Result<Response<'a>> {
        Ok(flatbuffers::size_prefixed_root::<Response>(&bytes)?)
    }

}
