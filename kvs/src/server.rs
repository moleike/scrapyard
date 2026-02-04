use std::{
    io::Read,
    net::{TcpListener, TcpStream},
};
use tracing::{debug, error, info, trace};

use crate::error::Result;
use crate::messages::{size_prefixed_root_as_request, Command, Request};

pub fn run(listener: TcpListener) -> () {
    for stream in listener.incoming() {
        let stream = stream.unwrap();

        info!("Connection established!");

        handle_client(stream).unwrap()
    }
}

pub fn handle_client(mut stream: TcpStream) -> Result<()> {
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf)?;

    let size = u32::from_le_bytes(size_buf) as usize;

    let mut msg_buf = vec![0u8; size];
    stream.read_exact(&mut msg_buf)?;

    let mut full_buffer = Vec::with_capacity(size + 4);
    full_buffer.extend_from_slice(&size_buf);
    full_buffer.extend_from_slice(&msg_buf);

    match size_prefixed_root_as_request(&full_buffer) {
        Ok(req) => {
            debug!("Valid Request Received: {:?}", req.command_type());
            handle_command(req)
        }
        Err(e) => {
            // This is where you catch "Invalid Flatbuffer" errors!
            error!("Invalid Flatbuffer error: {:?}", e);
            panic!()
        }
    }
}

fn handle_command(request: Request) -> Result<()> {
    match request.command_type() {
        Command::Set => {
            if let Some(op) = request.command_as_set() {
                let key = op.key().unwrap_or("");
                let val = op.value().unwrap_or("");
                trace!("Set: {} = {}", key, val);
            }
        }
        Command::Delete => {
            if let Some(op) = request.command_as_delete() {
                trace!("Delete: {}", op.key().unwrap_or(""));
            }
        }
        Command::Get => {
            if let Some(op) = request.command_as_get() {
                trace!("Get: {}", op.key().unwrap_or(""));
            }
        }
        Command::NONE => error!("No command provided"),
        _ => error!("Unknown command variant"),
    }
    Ok(())
}
