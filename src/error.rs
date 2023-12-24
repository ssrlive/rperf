#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("String error: {0}")]
    String(String),
}

impl From<Error> for std::io::Error {
    fn from(e: Error) -> Self {
        match e {
            Error::Io(e) => e,
            _ => std::io::Error::new(std::io::ErrorKind::Other, e),
        }
    }
}

impl From<&str> for Error {
    fn from(s: &str) -> Self {
        Error::String(s.to_string())
    }
}

impl From<String> for Error {
    fn from(s: String) -> Self {
        Error::String(s)
    }
}

impl From<&String> for Error {
    fn from(s: &String) -> Self {
        Error::String(s.to_string())
    }
}

pub type BoxResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync + 'static>>;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[macro_export]
macro_rules! error_gen {
    ($fmt:literal) => {
        $crate::error::Error::from(format!($fmt))
    };
    ($e:expr) => {
        $crate::error::Error::from($e)
    };
    ($fmt:literal, $($arg:tt)+) => {
        $crate::error::Error::from(format!($fmt, $($arg)+))
    };
}
