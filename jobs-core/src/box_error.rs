/// A simple error box that provides no additional formatting utilities
pub struct BoxError {
    error: Box<dyn std::error::Error + Send + Sync>,
}

impl std::fmt::Debug for BoxError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(f)
    }
}

impl std::fmt::Display for BoxError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(f)
    }
}

impl<E> From<E> for BoxError
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn from(error: E) -> Self {
        BoxError {
            error: Box::new(error),
        }
    }
}

impl From<BoxError> for Box<dyn std::error::Error + Send + Sync> {
    fn from(value: BoxError) -> Self {
        value.error
    }
}

impl From<BoxError> for Box<dyn std::error::Error + Send> {
    fn from(value: BoxError) -> Self {
        value.error
    }
}

impl From<BoxError> for Box<dyn std::error::Error> {
    fn from(value: BoxError) -> Self {
        value.error
    }
}

impl AsRef<dyn std::error::Error + Send + Sync> for BoxError {
    fn as_ref(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
        self.error.as_ref()
    }
}

impl AsRef<dyn std::error::Error + Send> for BoxError {
    fn as_ref(&self) -> &(dyn std::error::Error + Send + 'static) {
        self.error.as_ref()
    }
}

impl AsRef<dyn std::error::Error> for BoxError {
    fn as_ref(&self) -> &(dyn std::error::Error + 'static) {
        self.error.as_ref()
    }
}

impl std::ops::Deref for BoxError {
    type Target = dyn std::error::Error + Send + Sync;

    fn deref(&self) -> &Self::Target {
        self.error.as_ref()
    }
}
