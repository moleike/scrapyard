#[allow(unused_imports, clippy::extra_unused_lifetimes, clippy::missing_safety_doc, clippy::needless_lifetimes)]
#[rustfmt::skip]
mod messages_generated;

use std::{io::Read, ops::Deref};

use flatbuffers::{InvalidFlatbuffer, Verifiable};
pub use messages_generated::messages;

use self::messages::*;

pub struct OwnedFlatBuffer<T> {
    bytes: Vec<u8>,
    _marker: std::marker::PhantomData<T>,
}

impl<'a, T> OwnedFlatBuffer<T>
where
    T: flatbuffers::Follow<'a> + Verifiable, // Ensures T can be read from a buffer
{
    pub fn get_root(&'a self) -> Result<T::Inner, InvalidFlatbuffer> {
        flatbuffers::size_prefixed_root::<T>(&self.bytes)
    }
}

impl <'a, T> Deref for OwnedFlatBuffer<T>
where
    T: flatbuffers::Follow<'a> + Verifiable,
{
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.bytes
    }
}


pub fn serialize_request_get<'a>(key: &str) -> OwnedFlatBuffer<Request<'a>> {
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

    OwnedFlatBuffer {
        bytes: builder.finished_data().to_vec(),
        _marker: std::marker::PhantomData,
    }
}

pub fn serialize_request_set<'a>(key: &str, val: &str) -> OwnedFlatBuffer<Request<'a>> {
    let mut builder = flatbuffers::FlatBufferBuilder::new();

    let key_off = builder.create_string(key);
    let val_off = builder.create_string(val);

    let set_op = Set::create(
        &mut builder,
        &SetArgs {
            key: Some(key_off),
            value: Some(val_off),
        },
    );

    let req = Request::create(
        &mut builder,
        &RequestArgs {
            command_type: Command::Set,
            command: Some(set_op.as_union_value()),
        },
    );

    builder.finish_size_prefixed(req, None);

    OwnedFlatBuffer {
        bytes: builder.finished_data().to_vec(),
        _marker: std::marker::PhantomData,
    }
}

pub fn serialize_request_delete(key: &str) -> OwnedFlatBuffer<Request<'_>> {
    let mut builder = flatbuffers::FlatBufferBuilder::new();

    let key_off = builder.create_string(key);

    let delete_op = Delete::create(&mut builder, &DeleteArgs { key: Some(key_off) });

    let req = Request::create(
        &mut builder,
        &RequestArgs {
            command_type: Command::Delete,
            command: Some(delete_op.as_union_value()),
        },
    );

    builder.finish_size_prefixed(req, None);

    OwnedFlatBuffer {
        bytes: builder.finished_data().to_vec(),
        _marker: std::marker::PhantomData,
    }
}

pub fn serialize_response_value<'a>(val: &str) -> OwnedFlatBuffer<Response<'a>> {
    let mut builder = flatbuffers::FlatBufferBuilder::new();

    let v = builder.create_string(val);
    let gv = GetValue::create(&mut builder, &GetValueArgs { value: Some(v) });
    let res = Response::create(
        &mut builder,
        &ResponseArgs {
            reply_type: Reply::GetValue,
            reply: Some(gv.as_union_value()),
        },
    );

    builder.finish_size_prefixed(res, None);

    OwnedFlatBuffer {
        bytes: builder.finished_data().to_vec(),
        _marker: std::marker::PhantomData,
    }
}

pub fn serialize_response_success<'a>() -> OwnedFlatBuffer<Response<'a>> {
    let mut builder = flatbuffers::FlatBufferBuilder::new();
    let s = Success::create(&mut builder, &SuccessArgs::default());
    let res = Response::create(
        &mut builder,
        &ResponseArgs {
            reply_type: Reply::Success,
            reply: Some(s.as_union_value()),
        },
    );

    builder.finish_size_prefixed(res, None);

    OwnedFlatBuffer {
        bytes: builder.finished_data().to_vec(),
        _marker: std::marker::PhantomData,
    }
}

pub fn serialize_response_failure<'a>(code: ErrorCode) -> OwnedFlatBuffer<Response<'a>> {
    let mut builder = flatbuffers::FlatBufferBuilder::new();
    let f = Failure::create(&mut builder, &FailureArgs { code });
    let res = Response::create(
        &mut builder,
        &ResponseArgs {
            reply_type: Reply::Failure,
            reply: Some(f.as_union_value()),
        },
    );

    builder.finish_size_prefixed(res, None);

    OwnedFlatBuffer {
        bytes: builder.finished_data().to_vec(),
        _marker: std::marker::PhantomData,
    }
}

pub fn read<R: Read, T>(input: &mut R) -> crate::Result<OwnedFlatBuffer<T>> {
    let mut size_buf = [0u8; 4];
    input.read_exact(&mut size_buf)?;

    let size = u32::from_le_bytes(size_buf) as usize;

    let mut msg_buf = vec![0u8; size];
    input.read_exact(&mut msg_buf)?;

    let mut full_buffer = Vec::with_capacity(size + 4);
    full_buffer.extend_from_slice(&size_buf);
    full_buffer.extend_from_slice(&msg_buf);

    Ok(OwnedFlatBuffer {
        bytes: full_buffer,
        _marker: std::marker::PhantomData,
    })
}
