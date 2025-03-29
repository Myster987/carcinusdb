use crate::error::DatabaseError;

pub fn validate_hostname(input: &str) -> Result<String, DatabaseError> {
    let validate: Vec<_> = input.split(".").collect();

    if validate.len() != 4 {
        return Err(DatabaseError::InvalidHostname {
            msg: "hostname does not consists of 4 numbers betweeen 0 and 255.".to_string(),
            hostname: input.to_string(),
        });
    }

    for num in validate {
        let _ = num
            .parse::<u8>()
            .map_err(|_| DatabaseError::InvalidHostname {
                msg: format!("{num} is invalid."),
                hostname: input.to_string(),
            })?;
    }

    Ok(input.to_string())
}
