use std::{
    io::{Read, Write},
    net::{TcpStream, ToSocketAddrs},
};

use crate::{messages::messages::{
    size_prefixed_root_as_response, Command, ErrorCode, Get, GetArgs, Reply, Request, RequestArgs, Response,
}, Error::ServerError};

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
        let mut builder = flatbuffers::FlatBufferBuilder::new();
        let key_off = builder.create_string(key);

        let get_op = Get::create(&mut builder, &GetArgs { key: Some(key_off) });

        let req = Request::create(
            &mut builder,
            &RequestArgs {
                command_type: Command::Get,
                command: Some(get_op.as_union_value()),
            },
        );

        builder.finish_size_prefixed(req, None);
        self.stream.write_all(builder.finished_data())?;

        let mut size_buf = [0u8; 4];
        self.stream.read_exact(&mut size_buf)?;
        let size = u32::from_le_bytes(size_buf) as usize;

        // Safety check: Prevent massive allocations from corrupt data
        // if size > 10 * 1024 * 1024 { return Err("Response too large".into()); }

        let mut msg_buf = vec![0u8; size + 4];
        msg_buf[..4].copy_from_slice(&size_buf); // Verifier needs the prefix too
        self.stream.read_exact(&mut msg_buf[4..])?;

        let response = flatbuffers::size_prefixed_root::<Response>(&msg_buf)?;

        // --- 4. VERIFY & EXTRACT DATA ---
        match response.reply_type() {
            Reply::GetValue => {
                let get_val_table = response.reply_as_get_value().ok_or(ServerError)?;

                let val = get_val_table.value();
                Some(val.unwrap_or("").to_string()).ok_or(ServerError)
            }
            Reply::Failure => {
                let fail = response.reply_as_failure().ok_or(ServerError)?;
                match fail.code() {
                    ErrorCode::NotFound => Err(crate::Error::KeyNotFound),
                    _ => Err(ServerError),
                }
            }
            _ => Err(ServerError),
        }
    }
}
