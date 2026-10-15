#[allow(warnings)]
#[allow(clippy::all)]
mod generated {
    #[path = ""]
    pub mod ark {
        #[path = "ark.v1.rs"]
        pub mod v1;
    }
}

pub mod client;

mod error;
mod types;
mod vtxo_limiter;

pub use client::*;
pub use error::Error;
pub use vtxo_limiter::http_status_backs_off;
pub use vtxo_limiter::status_backs_off;
pub use vtxo_limiter::vtxo_request_permit;
pub use vtxo_limiter::VtxoRequestPermit;

#[cfg(feature = "test-utils")]
pub mod test_utils;
