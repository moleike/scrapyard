#[allow(unused_imports, clippy::extra_unused_lifetimes, clippy::missing_safety_doc, clippy::needless_lifetimes)]
#[rustfmt::skip]
mod messages_generated;

pub use messages_generated::messages;

use self::messages::*;

pub fn serialize_request_get(key: &str) -> Vec<u8> {
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
    builder.finished_data().to_vec()
}

pub fn serialize_request_set(key: &str, val: &str) -> Vec<u8> {
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
    builder.finished_data().to_vec()
}

pub fn serialize_request_delete(key: &str) -> Vec<u8> {
    let mut builder = flatbuffers::FlatBufferBuilder::new();

    let key_off = builder.create_string(key);

    let delete_op = Delete::create(
        &mut builder,
        &DeleteArgs {
            key: Some(key_off),
        },
    );

    let req = Request::create(
        &mut builder,
        &RequestArgs {
            command_type: Command::Delete,
            command: Some(delete_op.as_union_value()),
        },
    );

    builder.finish_size_prefixed(req, None);
    builder.finished_data().to_vec()
}

pub fn serialize_response_value(val: &str) -> Vec<u8> {
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

pub fn serialize_response_success() -> Vec<u8> {
    let mut b = flatbuffers::FlatBufferBuilder::new();
    let s = Success::create(&mut b, &SuccessArgs::default());
    let res = Response::create(
        &mut b,
        &ResponseArgs {
            reply_type: Reply::Success,
            reply: Some(s.as_union_value()),
        },
    );
    b.finish_size_prefixed(res, None);
    b.finished_data().to_vec()
}

pub fn serialize_response_failure(code: ErrorCode) -> Vec<u8> {
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
