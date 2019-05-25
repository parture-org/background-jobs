use failure::Fail;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, Fail)]
pub enum Error {
    #[fail(display = "Error in database: {}", _0)]
    Sled(#[cause] sled::Error),

    #[fail(display = "Failed to deserialize data")]
    Deserialize,

    #[fail(display = "Failed to serialize data")]
    Serialize,
}

impl From<sled::Error> for Error {
    fn from(e: sled::Error) -> Self {
        Error::Sled(e)
    }
}
