//! General information about CarcinusDB instance.
//! # Design
//! 
//! We need a way to store thinks like version, databases inside and other.
//! The solution which first comes to my mind is file with fixed size header that stores general information 
//! and after this header, there is list of blocks that contains informations about databases.
//! 


pub struct MetadataHeader {
    version: u32,
       
}