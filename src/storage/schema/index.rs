use crate::storage::Oid;

pub struct Index {
    oid: Oid,
    table_oid: Oid,
    name: String,
}
