use std::error::Error as StdError;
use std::fmt;

type Source = Box<dyn StdError + Send + Sync + 'static>;

pub struct Error {
    inner: ErrorImpl,
}

struct ErrorImpl {
    kind: Kind,
    source: Option<Source>,
}

#[derive(Debug)]
enum Kind {
    Connect,
    NotConnected,
    Request,
    Conversion,
    EventStreamDisconnect,
    EventStream,
    ServerInfoChanged,
}

impl Error {
    fn new(kind: Kind) -> Self {
        Self {
            inner: ErrorImpl { kind, source: None },
        }
    }

    pub(crate) fn with(mut self, source: impl Into<Source>) -> Self {
        self.inner.source = Some(source.into());
        self
    }

    pub(crate) fn connect(source: impl Into<Source>) -> Self {
        Error::new(Kind::Connect).with(source)
    }

    pub(crate) fn not_connected() -> Self {
        Error::new(Kind::NotConnected)
    }

    pub(crate) fn request(source: impl Into<Source>) -> Self {
        Error::new(Kind::Request).with(source)
    }

    pub(crate) fn conversion(source: impl Into<Source>) -> Self {
        Error::new(Kind::Conversion).with(source)
    }

    pub(crate) fn event_stream_disconnect() -> Self {
        Error::new(Kind::EventStreamDisconnect)
    }

    pub(crate) fn event_stream(source: impl Into<Source>) -> Self {
        Error::new(Kind::EventStream).with(source)
    }

    pub(crate) fn server_info_changed(source: impl Into<Source>) -> Self {
        Error::new(Kind::ServerInfoChanged).with(source)
    }

    pub fn is_server_info_changed(&self) -> bool {
        matches!(self.inner.kind, Kind::ServerInfoChanged)
    }

    /// Returns `true` if the server rejected the request because the SDK
    /// version is too old.
    pub fn is_version_mismatch(&self) -> bool {
        if let Some(source) = &self.inner.source {
            if let Some(status) = source.downcast_ref::<tonic::Status>() {
                return status.code() == tonic::Code::FailedPrecondition
                    && status.message().contains("BUILD_VERSION_TOO_OLD");
            }
        }
        false
    }

    /// Returns `true` if the server answered, if only to refuse this request,
    /// rather than failing in a way that means callers should back off.
    pub(crate) fn server_answered(&self) -> bool {
        match self.inner.kind {
            // The digest check failed and `/info` answered since.
            Kind::ServerInfoChanged => true,
            Kind::Request => self
                .inner
                .source
                .as_ref()
                .and_then(|source| source.downcast_ref::<tonic::Status>())
                .is_some_and(|status| !crate::vtxo_limiter::status_backs_off(status.code())),
            Kind::Connect
            | Kind::NotConnected
            | Kind::Conversion
            | Kind::EventStreamDisconnect
            | Kind::EventStream => false,
        }
    }

    /// Returns `true` if the server rejected the request because the cached
    /// `/info` digest is stale.
    pub(crate) fn is_digest_mismatch(&self) -> bool {
        if let Some(source) = &self.inner.source {
            if let Some(status) = source.downcast_ref::<tonic::Status>() {
                return status.code() == tonic::Code::FailedPrecondition
                    && (status.message().contains("DIGEST_MISMATCH")
                        || status.message().contains("invalid digest header"));
            }
        }
        false
    }

    fn description(&self) -> &str {
        match &self.inner.kind {
            Kind::Connect => "failed to connect to Ark server",
            Kind::NotConnected => "no connection to Ark server",
            Kind::Request => "request failed",
            Kind::Conversion => "failed to convert between types",
            Kind::EventStreamDisconnect => "got disconnected from event stream",
            Kind::EventStream => "error via event stream",
            Kind::ServerInfoChanged => "Ark server info changed while processing the request. Server info was refreshed, but the failed operation was not retried. Rebuild the request and retry if safe",
        }
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut f = f.debug_tuple("ark_grpc::Error");

        f.field(&self.inner.kind);

        if let Some(source) = &self.inner.source {
            f.field(source);
        }

        f.finish()
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.description())?;
        if let Some(source) = self.source() {
            f.write_str(&source.to_string())?;
        }

        Ok(())
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.inner
            .source
            .as_ref()
            .map(|source| &**source as &(dyn StdError + 'static))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_version_mismatch_true_for_matching_status() {
        let status = tonic::Status::failed_precondition("BUILD_VERSION_TOO_OLD");
        let err = Error::request(status);
        assert!(err.is_version_mismatch());
    }

    #[test]
    fn is_version_mismatch_false_for_other_failed_precondition() {
        let status = tonic::Status::failed_precondition("something else");
        let err = Error::request(status);
        assert!(!err.is_version_mismatch());
    }

    #[test]
    fn is_version_mismatch_false_for_other_code() {
        let status = tonic::Status::internal("BUILD_VERSION_TOO_OLD");
        let err = Error::request(status);
        assert!(!err.is_version_mismatch());
    }

    #[test]
    fn is_version_mismatch_false_for_non_tonic_error() {
        let err = Error::request("some string error");
        assert!(!err.is_version_mismatch());
    }

    #[test]
    fn is_version_mismatch_false_when_no_source() {
        let err = Error::not_connected();
        assert!(!err.is_version_mismatch());
    }

    #[test]
    fn is_digest_mismatch_true_for_matching_status() {
        let status = tonic::Status::failed_precondition("invalid digest header");
        let err = Error::request(status);
        assert!(err.is_digest_mismatch());
    }

    #[test]
    fn is_digest_mismatch_true_for_error_code_in_message() {
        let status = tonic::Status::failed_precondition("DIGEST_MISMATCH");
        let err = Error::request(status);
        assert!(err.is_digest_mismatch());
    }

    #[test]
    fn is_digest_mismatch_false_for_other_failed_precondition() {
        let status = tonic::Status::failed_precondition("something else");
        let err = Error::request(status);
        assert!(!err.is_digest_mismatch());
    }

    #[test]
    fn server_answered_only_for_refusals_of_the_request() {
        for status in [
            tonic::Status::invalid_argument("bad outpoint"),
            tonic::Status::not_found("no such script"),
            tonic::Status::failed_precondition("BUILD_VERSION_TOO_OLD"),
        ] {
            assert!(Error::request(status).server_answered());
        }
        assert!(
            Error::server_info_changed(Error::request(tonic::Status::failed_precondition(
                "DIGEST_MISMATCH"
            )))
            .server_answered()
        );
        for status in [
            tonic::Status::unavailable("transport error"),
            tonic::Status::deadline_exceeded("GetVtxos exceeded its 20s budget"),
            tonic::Status::resource_exhausted("slow down"),
            tonic::Status::internal("panic"),
            tonic::Status::permission_denied("blocked"),
        ] {
            assert!(!Error::request(status).server_answered());
        }
        assert!(!Error::request("some string error").server_answered());
        assert!(!Error::connect("refused").server_answered());
        assert!(!Error::conversion("missing VTXO outpoint").server_answered());
        assert!(!Error::not_connected().server_answered());
    }
}
