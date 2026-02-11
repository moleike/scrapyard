use std::{
    env::current_dir,
    io::{Read, Write},
    net::{TcpListener, TcpStream, ToSocketAddrs},
};
use tracing::{debug, error, info, trace};

use crate::messages::messages::{
    Command, ErrorCode, Failure, FailureArgs, GetValue, GetValueArgs, Reply, Request, Response,
    ResponseArgs,
};
use crate::{
    engine::{kvs::KvStore, KvsEngine},
    error::Result,
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
                    Ok(stream.write_all(&response_data)?)
                } else {
                    Ok(())
                }

            }
            Err(e) => {
                // This is where you catch "Invalid Flatbuffer" errors!
                error!("Invalid Flatbuffer error: {:?}", e);
                panic!()
            }
        }
    }

    fn handle_command(&mut self, request: Request) -> crate::Result<Option<Vec<u8>>> {
        match request.command_type() {
            Command::Set => {
                if let Some(op) = request.command_as_set() {
                    let key = op.key().unwrap_or("");
                    let val = op.value().unwrap_or("");
                    trace!("Set: {} = {}", key, val);

                    Ok(None)
                } else {
                    Ok(None)
                }
            }
            Command::Delete => {
                if let Some(op) = request.command_as_delete() {
                    trace!("Delete: {}", op.key().unwrap_or(""));

                    Ok(None)
                } else {
                    Ok(None)
                }
            }
            Command::Get => {
                if let Some(op) = request.command_as_get() {
                    trace!("Get: {}", op.key().unwrap_or(""));

                    let key = op.key().unwrap();

                    let response_data = match self.engine.get(key.to_string()) {
                        Ok(Some(value)) => Self::serialize_response_value(&value),
                        _ => {
                            Self::serialize_response_failure(ErrorCode::NotFound)
                        }
                    };

                    Ok(Some(response_data))
                } else {
                    Ok(None)
                }
            }
            Command::NONE => {
                error!("No command provided");
                Ok(None)
            },
            _ => {
                error!("Unknown command variant");
                Ok(None)
            }

        }
    }

    fn serialize_response_value(val: &str) -> Vec<u8> {
        let mut b = flatbuffers::FlatBufferBuilder::new();
        let v = b.create_string(val);
        let gv = GetValue::create(&mut b, &GetValueArgs { value: Some(v) });
        let res = Response::create(
            &mut b,
            &ResponseArgs {
                reply_type: Reply::GetValue,
                reply: Some(gv.as_union_value()),
            },
        );
        b.finish_size_prefixed(res, None);
        b.finished_data().to_vec()
    }

    fn serialize_response_failure(code: ErrorCode) -> Vec<u8> {
        let mut b = flatbuffers::FlatBufferBuilder::new();
        let f = Failure::create(&mut b, &FailureArgs { code });
        let res = Response::create(
            &mut b,
            &ResponseArgs {
                reply_type: Reply::Failure,
                reply: Some(f.as_union_value()),
            },
        );
        b.finish_size_prefixed(res, None);
        b.finished_data().to_vec()
    }
}
