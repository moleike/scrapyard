use std::{
    env::current_dir,
    io::{Read, Write},
    net::{TcpListener, TcpStream, ToSocketAddrs},
};
use tracing::{error, info, trace};

use crate::engine::{kvs::KvStore, KvsEngine};
use crate::{
    messages::{
        self,
        messages::{Command, ErrorCode, Request},
    },
    Error,
};

pub struct Server {
    engine: Box<dyn KvsEngine>,
    listener: TcpListener,
}

impl Server {
    pub fn new<T: ToSocketAddrs>(
        addr: T,
        engine: Option<Box<dyn KvsEngine>>,
    ) -> crate::Result<Self> {
        let listener = TcpListener::bind(addr)?;

        let engine = engine.unwrap_or(Box::new(KvStore::open(current_dir()?)?));

        Ok(Self { engine, listener })
    }

    pub fn run(&mut self) -> crate::Result<()> {
        let listener = self.listener.try_clone()?;

        for stream in listener.incoming() {
            let stream = stream?;

            info!("Connection established!");

            self.handle_client(stream)?
        }

        Ok(())
    }

    fn handle_client(&mut self, mut stream: TcpStream) -> crate::Result<()> {
        let mut size_buf = [0u8; 4];
        stream.read_exact(&mut size_buf)?;

        let size = u32::from_le_bytes(size_buf) as usize;

        let mut msg_buf = vec![0u8; size];
        stream.read_exact(&mut msg_buf)?;

        let mut full_buffer = Vec::with_capacity(size + 4);
        full_buffer.extend_from_slice(&size_buf);
        full_buffer.extend_from_slice(&msg_buf);

        match flatbuffers::size_prefixed_root::<Request>(&full_buffer) {
            Ok(req) => {
                info!("Valid Request Received: {:?}", req.command_type());

                if let Some(response_data) = self.handle_command(req)? {
                    stream.write_all(&response_data)?;
                }

                Ok(())
            }
            Err(e) => {
                // This is where you catch "Invalid Flatbuffer" errors!
                error!("Invalid Flatbuffer error: {:?}", e);

                Err(Error::ProtocolError(e))
            }
        }
    }

    fn handle_command(&mut self, request: Request) -> crate::Result<Option<Vec<u8>>> {
        match request.command_type() {
            Command::Get => {
                if let Some(op) = request.command_as_get() {
                    let key = op.key().unwrap();

                    trace!("Get: {}", key);

                    let response_data = match self.engine.get(key.to_string()) {
                        Ok(Some(value)) => messages::serialize_response_value(&value),
                        _ => messages::serialize_response_failure(ErrorCode::NotFound),
                    };

                    Ok(Some(response_data))
                } else {
                    Ok(None)
                }
            }
            Command::Set => {
                if let Some(op) = request.command_as_set() {
                    let key = op.key().unwrap();
                    let val = op.value().unwrap();

                    trace!("Set: {} = {}", key, val);

                    let response_data = match self.engine.set(key.to_string(), val.to_string()) {
                        Ok(()) => messages::serialize_response_success(),
                        Err(_) => messages::serialize_response_failure(ErrorCode::Unknown),
                    };
                    Ok(Some(response_data))
                } else {
                    Ok(None)
                }
            }
            Command::Delete => {
                if let Some(op) = request.command_as_delete() {
                    let key = op.key().unwrap();

                    trace!("Delete: {}", key);

                    let response_data = match self.engine.remove(key.to_string()) {
                        Ok(()) => messages::serialize_response_success(),
                        Err(Error::KeyNotFound) => {
                            messages::serialize_response_failure(ErrorCode::NotFound)
                        }
                        Err(_) => messages::serialize_response_failure(ErrorCode::Unknown),
                    };
                    Ok(Some(response_data))
                } else {
                    Ok(None)
                }
            }
            Command::NONE => {
                error!("No command provided");
                Ok(None)
            }
            _ => {
                error!("Unknown command variant");
                Ok(None)
            }
        }
    }
}
