/// storage engine
pub trait KvsEngine {
    fn get(&mut self, key: String) -> crate::Result<Option<String>>;

    fn set(&mut self, key: String, value: String) -> crate::Result<()>;

    fn remove(&mut self, key: String) -> crate::Result<()>;
}

pub mod kvs;
