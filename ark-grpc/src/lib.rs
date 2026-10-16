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
pub use vtxo_limiter::set_vtxo_request_budget;
pub use vtxo_limiter::vtxo_request_permit;
pub use vtxo_limiter::with_vtxo_request_priority;
pub use vtxo_limiter::VtxoBudgetError;
pub use vtxo_limiter::VtxoRequestBudget;
pub use vtxo_limiter::VtxoRequestPermit;
pub use vtxo_limiter::VtxoRequestPriority;

#[cfg(feature = "test-utils")]
pub mod test_utils;
