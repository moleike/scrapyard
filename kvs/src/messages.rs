#[allow(unused_imports, clippy::extra_unused_lifetimes, clippy::missing_safety_doc, clippy::needless_lifetimes)]
#[rustfmt::skip]
mod messages_generated;

pub use messages_generated::messages;

use self::messages::*;

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
