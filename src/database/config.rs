use crate::storage::page::ConfigPage;

#[derive(Debug)]
pub struct Config {
    version: u32,
    page_size: u16,
}

impl From<ConfigPage> for Config {
    fn from(ConfigPage { version, page_size }: ConfigPage) -> Self {
        Self {
            version: version,
            page_size: page_size,
        }
    }
}
