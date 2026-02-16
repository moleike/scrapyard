use std::{
    env::current_dir,
    io::{Read, Write},
    net::{TcpListener, TcpStream, ToSocketAddrs},
};
use tracing::{error, info, trace};

use crate::{
    engine::KvsEngine,
    messages::{messages::{Command, Response}},
};
use crate::{
    messages::{
        self,
        messages::{ErrorCode, Request},
        OwnedFlatBuffer,
    },
    Error,
};

pub struct Server {
    engine: Box<dyn KvsEngine>,
    listener: TcpListener,
}

impl Server {
    pub fn new<T: ToSocketAddrs>(addr: T, engine: Box<dyn KvsEngine>) -> crate::Result<Self> {
        let listener = TcpListener::bind(addr)?;

        Ok(Self { engine, listener })
    }

    pub fn run(&mut self) -> crate::Result<()> {
        let listener = self.listener.try_clone()?;

        for stream in listener.incoming() {
            let stream = stream?;

            trace!("Connection established!");

            self.handle_client(stream)?
        }

        Ok(())
    }

    fn handle_client(&mut self, mut stream: TcpStream) -> crate::Result<()> {
        let buf = messages::read::<TcpStream, Request>(&mut stream)?;
        let req = buf.get_root()?;

        match self.handle_command(req)? {
            Some(response_data) => {
                stream.write_all(&response_data.as_bytes())?;
            }
            _ => (),
        }

        Ok(())
    }

    fn handle_command(
        &mut self,
        request: Request,
    ) -> crate::Result<Option<OwnedFlatBuffer<Response<'_>>>> {
        match request.command_type() {
            Command::Get if let Some(op) = request.command_as_get() => {
                let key = op.key().unwrap();

                trace!("Get: {}", key);

                let response_data = match self.engine.get(key.to_string()) {
                    Ok(Some(value)) => messages::serialize_response_value(&value),
                    _ => messages::serialize_response_failure(ErrorCode::NotFound),
                };

                Ok(Some(response_data))
            }
            Command::Set if let Some(op) = request.command_as_set() => {
                let key = op.key().unwrap();
                let val = op.value().unwrap();

                trace!("Set: {} = {}", key, val);

                let response_data = match self.engine.set(key.to_string(), val.to_string()) {
                    Ok(()) => messages::serialize_response_success(),
                    Err(_) => messages::serialize_response_failure(ErrorCode::Unknown),
                };
                Ok(Some(response_data))
            }
            Command::Delete if let Some(op) = request.command_as_delete() => {
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
