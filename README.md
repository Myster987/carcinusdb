# CarcinusDB

## Storage architecture:

**Overview:**

```
carcinusdb/     <- root directory
    metadata    <- general information about db (version, databases, etc)
    data/       <- place where all data is stored
        example_db_name/    <- each db is stored in it's own directory  
            schema                      <- holds general information about tables and indexes
            table                       <- stores more specific information about tables
            index                       <- stores more specific information about indexes   
            table_id.block_number       <- each table is divided into blocks, with default size of 1GB
            index_id.block_number       <- each index is also diveded into block
            toast                       <- future plan 

```         