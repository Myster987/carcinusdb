pub trait AnyBlob {
    fn as_slice(&self) -> &[u8];
}

impl AnyBlob for Vec<u8> {
    fn as_slice(&self) -> &[u8] {
        self.as_slice()
    }
}

impl AnyBlob for &[u8] {
    fn as_slice(&self) -> &[u8] {
        self
    }
}
