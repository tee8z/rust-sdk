//! One request in flight per endpoint, shared by every client in this process.
//! This is a local safety budget, not an assertion about the operator's capacity.

use rand::Rng;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::OwnedMutexGuard;
use tokio::time::Instant;

const SPACING: Duration = Duration::from_secs(2);

struct State {
    next: Instant,
    failures: u32,
}

impl Default for State {
    fn default() -> Self {
        Self {
            next: Instant::now(),
            failures: 0,
        }
    }
}

type Limiter = Arc<AsyncMutex<State>>;
static LIMITERS: OnceLock<Mutex<HashMap<String, Limiter>>> = OnceLock::new();

/// Reserve a GetVtxos attempt, including REST requests for the same endpoint.
///
/// Clients sharing an endpoint share one in-flight request and a two-second
/// gap after completion. Failures and cancelled in-flight requests back off with
/// equal jitter: 10–20 seconds, doubling to 150–300 seconds. Waiting is cancellable
/// and does not dispatch or count a request. Keep this permit until the server
/// answers, then call `succeeded`, also for an answer that only refuses this
/// request (`status_backs_off`, `http_status_backs_off`) or that cannot be decoded.
/// Dropping it otherwise records a failure.
///
/// The budget is per process. Separate processes and hosts need their own budgets.
pub async fn vtxo_request_permit(endpoint: &str) -> VtxoRequestPermit {
    // Retain strong references: constructing a fresh client must not reset pacing.
    let limiter = {
        let mut limiters = LIMITERS
            .get_or_init(Mutex::default)
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        limiters
            .entry(endpoint.trim_end_matches('/').to_ascii_lowercase())
            .or_insert_with(|| Arc::new(AsyncMutex::new(State::default())))
            .clone()
    };
    acquire(limiter).await
}

async fn acquire(limiter: Limiter) -> VtxoRequestPermit {
    let state = limiter.lock_owned().await;
    tokio::time::sleep_until(state.next).await;
    VtxoRequestPermit {
        state,
        success: false,
    }
}

/// Whether a GetVtxos status means every caller should back off: the server may
/// be struggling, or it refuses us. A status that only says this request was wrong
/// is an answer, and the next request waits the usual spacing.
pub fn status_backs_off(code: tonic::Code) -> bool {
    !matches!(
        code,
        tonic::Code::InvalidArgument
            | tonic::Code::NotFound
            | tonic::Code::AlreadyExists
            | tonic::Code::FailedPrecondition
            | tonic::Code::OutOfRange
    )
}

/// `status_backs_off` for an HTTP status from the REST indexer.
pub fn http_status_backs_off(status: u16) -> bool {
    match status {
        200..=299 => false,
        401 | 403 | 408 | 429 => true,
        400..=499 => false,
        _ => true,
    }
}

/// Holds the shared budget through dispatch and response.
pub struct VtxoRequestPermit {
    state: OwnedMutexGuard<State>,
    success: bool,
}

impl VtxoRequestPermit {
    pub fn succeeded(mut self) {
        self.success = true;
    }
}

impl Drop for VtxoRequestPermit {
    fn drop(&mut self) {
        let delay = if self.success {
            self.state.failures = 0;
            SPACING
        } else {
            self.state.failures = self.state.failures.saturating_add(1);
            let ceiling = (20_u64 << self.state.failures.saturating_sub(1).min(4)).min(300);
            Duration::from_millis(rand::thread_rng().gen_range(ceiling * 500..=ceiling * 1000))
        };
        self.state.next = Instant::now() + delay;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    #[tokio::test(start_paused = true)]
    async fn a_hundred_concurrent_callers_are_serial_and_spaced_even_on_success() {
        let limiter = Arc::new(AsyncMutex::new(State::default()));
        let in_flight = Arc::new(AtomicUsize::new(0));
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..100 {
            let limiter = limiter.clone();
            let in_flight = in_flight.clone();
            tasks.spawn(async move {
                let permit = acquire(limiter).await;
                let started = Instant::now();
                assert_eq!(in_flight.fetch_add(1, Ordering::SeqCst), 0);
                tokio::time::sleep(Duration::from_millis(100)).await;
                assert_eq!(in_flight.fetch_sub(1, Ordering::SeqCst), 1);
                permit.succeeded();
                started
            });
        }
        let mut arrivals = Vec::new();
        while let Some(result) = tasks.join_next().await {
            arrivals.push(result.expect("limiter task must finish"));
        }
        arrivals.sort();
        assert_eq!(arrivals.len(), 100);
        for pair in arrivals.windows(2) {
            assert!(pair[1] - pair[0] >= Duration::from_millis(2100));
        }
        for start in &arrivals {
            assert!(
                arrivals
                    .iter()
                    .filter(|at| **at >= *start && **at < *start + Duration::from_secs(60))
                    .count()
                    <= 30
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn failures_and_cancelled_requests_back_off_globally_and_success_recovers() {
        let limiter = Arc::new(AsyncMutex::new(State::default()));
        let mut permit = acquire(limiter.clone()).await;
        for (low, high) in [
            (10, 20),
            (20, 40),
            (40, 80),
            (80, 160),
            (150, 300),
            (150, 300),
        ] {
            let completed = Instant::now();
            // Includes unwinding a timeout after dispatch: no success acknowledgement.
            drop(permit);
            permit = acquire(limiter.clone()).await;
            let elapsed = Instant::now() - completed;
            assert!(elapsed >= Duration::from_secs(low));
            assert!(elapsed <= Duration::from_secs(high));
        }
        let completed = Instant::now();
        permit.succeeded();
        let permit = acquire(limiter.clone()).await;
        assert_eq!(Instant::now() - completed, SPACING);
        let completed = Instant::now();
        drop(permit);
        acquire(limiter).await.succeeded();
        assert!((Duration::from_secs(10)..=Duration::from_secs(20))
            .contains(&(Instant::now() - completed)));
    }

    #[tokio::test(start_paused = true)]
    async fn cancelling_queued_requests_neither_dispatches_nor_extends_the_backoff() {
        let limiter = Arc::new(AsyncMutex::new(State::default()));
        drop(acquire(limiter.clone()).await);
        let completed = Instant::now();
        for _ in 0..5 {
            assert!(
                tokio::time::timeout(Duration::from_secs(1), acquire(limiter.clone()))
                    .await
                    .is_err()
            );
        }
        acquire(limiter).await.succeeded();
        assert!((Duration::from_secs(10)..=Duration::from_secs(20))
            .contains(&(Instant::now() - completed)));
    }

    #[tokio::test(start_paused = true)]
    async fn aborting_an_in_flight_task_preserves_the_endpoint_backoff_for_new_clients() {
        let endpoint = "http://cancelled-request.test";
        let (started, received) = tokio::sync::oneshot::channel();
        let request = tokio::spawn(async move {
            let _permit = vtxo_request_permit(endpoint).await;
            started.send(()).expect("test receiver must remain open");
            std::future::pending::<()>().await;
        });
        received.await.expect("request must acquire a permit");
        request.abort();
        assert!(request
            .await
            .expect_err("aborted task must be cancelled")
            .is_cancelled());
        let completed = Instant::now();
        // A new caller, including REST with a trailing slash, shares the state.
        vtxo_request_permit("http://cancelled-request.test/")
            .await
            .succeeded();
        assert!((Duration::from_secs(10)..=Duration::from_secs(20))
            .contains(&(Instant::now() - completed)));
    }

    #[test]
    fn only_trouble_at_the_server_or_a_refusal_of_us_backs_off() {
        use tonic::Code;
        for code in [
            Code::InvalidArgument,
            Code::NotFound,
            Code::AlreadyExists,
            Code::FailedPrecondition,
            Code::OutOfRange,
        ] {
            assert!(!status_backs_off(code), "{code:?}");
        }
        for code in [
            Code::Unknown,
            Code::Cancelled,
            Code::DeadlineExceeded,
            Code::PermissionDenied,
            Code::ResourceExhausted,
            Code::Aborted,
            Code::Unimplemented,
            Code::Internal,
            Code::Unavailable,
            Code::DataLoss,
            Code::Unauthenticated,
        ] {
            assert!(status_backs_off(code), "{code:?}");
        }
        for status in [200, 204, 400, 404, 409, 414, 422] {
            assert!(!http_status_backs_off(status), "{status}");
        }
        for status in [302, 401, 403, 408, 429, 500, 502, 503, 504] {
            assert!(http_status_backs_off(status), "{status}");
        }
    }
}
