//! One request in flight per endpoint, shared by every client in this process.
//! This is a local safety budget, not an assertion about the operator's capacity.

use rand::Rng;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
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
    budget: Option<Arc<dyn VtxoRequestBudget>>,
    deferred: [Option<(Instant, VtxoBudgetError)>; 2],
}

impl Default for State {
    fn default() -> Self {
        Self {
            next: Instant::now(),
            failures: 0,
            budget: None,
            deferred: [None, None],
        }
    }
}

type Limiter = Arc<AsyncMutex<State>>;
#[derive(Clone)]
struct Endpoint {
    limiter: Limiter,
    payments: Arc<AtomicUsize>,
}
static LIMITERS: OnceLock<Mutex<HashMap<String, Endpoint>>> = OnceLock::new();

/// Unclassified reads are background work and cannot consume payment reserves.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum VtxoRequestPriority {
    Payment,
    #[default]
    Background,
}

impl VtxoRequestPriority {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Payment => "payment",
            Self::Background => "background",
        }
    }

    fn index(self) -> usize {
        match self {
            Self::Payment => 0,
            Self::Background => 1,
        }
    }
}

tokio::task_local! { static REQUEST_PRIORITY: VtxoRequestPriority; }

/// Scope the priority to this future, including directly awaited SDK calls.
/// Spawned tasks default to background and must be scoped explicitly if needed.
pub async fn with_vtxo_request_priority<F: Future>(
    priority: VtxoRequestPriority,
    future: F,
) -> F::Output {
    REQUEST_PRIORITY.scope(priority, future).await
}

struct WaitingPayment(Arc<AtomicUsize>);
impl WaitingPayment {
    fn new(waiting: Arc<AtomicUsize>) -> Self {
        waiting.fetch_add(1, Ordering::SeqCst);
        Self(waiting)
    }
}
impl Drop for WaitingPayment {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// A shared, external budget. Reserve durably before returning success. A failed
/// or cancelled reservation must never cause a request to the indexer.
pub trait VtxoRequestBudget: Send + Sync {
    fn reserve(
        &self,
        priority: VtxoRequestPriority,
    ) -> Pin<Box<dyn Future<Output = Result<(), VtxoBudgetError>> + Send + '_>>;
}

#[derive(Debug, Clone)]
pub struct VtxoBudgetError {
    pub retry_after: Duration,
    pub message: String,
}

impl std::fmt::Display for VtxoBudgetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.message.fmt(f)
    }
}

impl std::error::Error for VtxoBudgetError {}

fn limiter(endpoint: &str) -> Endpoint {
    LIMITERS
        .get_or_init(Mutex::default)
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .entry(endpoint.trim_end_matches('/').to_ascii_lowercase())
        .or_insert_with(|| Endpoint {
            limiter: Arc::new(AsyncMutex::new(State::default())),
            payments: Arc::default(),
        })
        .clone()
}

/// Install a budget before starting callers. Existing and new clients for this
/// endpoint share it. Unavailable budgets fail closed; there is no local fallback.
pub async fn set_vtxo_request_budget(endpoint: &str, budget: Arc<dyn VtxoRequestBudget>) {
    limiter(endpoint).limiter.lock().await.budget = Some(budget);
}

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
/// Pacing is per process. Install [`set_vtxo_request_budget`] before starting
/// callers to enforce an additional durable budget shared across processes.
pub async fn vtxo_request_permit(endpoint: &str) -> Result<VtxoRequestPermit, VtxoBudgetError> {
    // Retain strong references: constructing a fresh client must not reset pacing.
    let endpoint = limiter(endpoint);
    let priority = REQUEST_PRIORITY
        .try_with(|priority| *priority)
        .unwrap_or_default();
    let _waiting = (priority == VtxoRequestPriority::Payment)
        .then(|| WaitingPayment::new(endpoint.payments.clone()));
    let state = endpoint.limiter.lock_owned().await;
    let yield_to_payment = || {
        if priority == VtxoRequestPriority::Background
            && endpoint.payments.load(Ordering::SeqCst) > 0
        {
            Err(VtxoBudgetError {
                retry_after: Duration::from_secs(1),
                message: "background GetVtxos read deferred for a waiting payment".into(),
            })
        } else {
            Ok(())
        }
    };
    yield_to_payment()?;
    if let Some((until, error)) = &state.deferred[priority.index()] {
        if *until > Instant::now() {
            return Err(VtxoBudgetError {
                retry_after: until.saturating_duration_since(Instant::now()),
                message: error.message.clone(),
            });
        }
    }
    tokio::time::sleep_until(state.next).await;
    yield_to_payment()?;
    let mut permit = VtxoRequestPermit {
        state,
        success: false,
        reserved: true,
    };
    if let Some(budget) = permit.state.budget.clone() {
        if let Err(error) = budget.reserve(priority).await {
            // Quota denials are cached per priority and return promptly. A
            // background denial cannot block the reserved payment capacity.
            permit.state.deferred[priority.index()] =
                Some((Instant::now() + error.retry_after, error.clone()));
            permit.reserved = false;
            return Err(error);
        }
    }
    permit.state.deferred[priority.index()] = None;
    Ok(permit)
}

#[cfg(test)]
async fn acquire(limiter: Limiter) -> VtxoRequestPermit {
    let state = limiter.lock_owned().await;
    tokio::time::sleep_until(state.next).await;
    VtxoRequestPermit {
        state,
        success: false,
        reserved: true,
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
    reserved: bool,
}

impl VtxoRequestPermit {
    pub fn succeeded(mut self) {
        self.success = true;
    }
}

impl Drop for VtxoRequestPermit {
    fn drop(&mut self) {
        if !self.reserved {
            return;
        }
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

    struct PaymentReserve;

    impl VtxoRequestBudget for PaymentReserve {
        fn reserve(
            &self,
            priority: VtxoRequestPriority,
        ) -> Pin<Box<dyn Future<Output = Result<(), VtxoBudgetError>> + Send + '_>> {
            Box::pin(async move {
                match priority {
                    VtxoRequestPriority::Payment => Ok(()),
                    VtxoRequestPriority::Background => Err(VtxoBudgetError {
                        retry_after: Duration::from_secs(240),
                        message: "capacity reserved for payments".into(),
                    }),
                }
            })
        }
    }

    #[tokio::test(start_paused = true)]
    async fn background_denial_does_not_block_payment_or_leak_priority_to_other_tasks() {
        let endpoint = "https://priority-reserve.test";
        set_vtxo_request_budget(endpoint, Arc::new(PaymentReserve)).await;
        assert!(vtxo_request_permit(endpoint).await.is_err());
        let start = Instant::now();
        with_vtxo_request_priority(VtxoRequestPriority::Payment, async {
            vtxo_request_permit(endpoint).await.unwrap().succeeded();
            assert!(tokio::spawn(async { vtxo_request_permit(endpoint).await })
                .await
                .unwrap()
                .is_err());
        })
        .await;
        assert_eq!(
            Instant::now(),
            start,
            "payment must not wait for the background cooldown"
        );
        assert!(
            vtxo_request_permit(endpoint).await.is_err(),
            "priority ends with the scoped future"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn waiting_payments_pass_queued_background_work_and_cancellation_releases_priority() {
        let endpoint = "https://priority-queue.test";
        let held = vtxo_request_permit(endpoint).await.unwrap();
        let background = tokio::spawn(async { vtxo_request_permit(endpoint).await });
        tokio::task::yield_now().await;
        let payment = tokio::spawn(with_vtxo_request_priority(
            VtxoRequestPriority::Payment,
            async {
                vtxo_request_permit(endpoint).await.unwrap().succeeded();
            },
        ));
        tokio::task::yield_now().await;
        held.succeeded();
        assert!(
            background.await.unwrap().is_err(),
            "queued background work must yield"
        );
        payment.await.unwrap();

        let held = vtxo_request_permit(endpoint).await.unwrap();
        let cancelled = tokio::spawn(with_vtxo_request_priority(
            VtxoRequestPriority::Payment,
            async {
                vtxo_request_permit(endpoint).await.unwrap().succeeded();
            },
        ));
        tokio::task::yield_now().await;
        cancelled.abort();
        assert!(cancelled.await.unwrap_err().is_cancelled());
        held.succeeded();
        vtxo_request_permit(endpoint).await.unwrap().succeeded();
    }

    struct DeniedBudget(AtomicUsize);

    impl VtxoRequestBudget for DeniedBudget {
        fn reserve(
            &self,
            _priority: VtxoRequestPriority,
        ) -> Pin<Box<dyn Future<Output = Result<(), VtxoBudgetError>> + Send + '_>> {
            Box::pin(async {
                self.0.fetch_add(1, Ordering::SeqCst);
                Err(VtxoBudgetError {
                    retry_after: Duration::from_secs(240),
                    message: "shared quota exhausted".into(),
                })
            })
        }
    }

    #[tokio::test(start_paused = true)]
    async fn shared_budget_denial_blocks_new_clients_and_cannot_be_bypassed_by_a_burst() {
        let endpoint = "https://shared-budget.test";
        let budget = Arc::new(DeniedBudget(AtomicUsize::new(0)));
        set_vtxo_request_budget(endpoint, budget.clone()).await;
        let result = vtxo_request_permit(endpoint).await;
        assert!(result.is_err());
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..100 {
            tasks.spawn(async {
                tokio::time::timeout(
                    Duration::from_secs(20),
                    vtxo_request_permit("https://shared-budget.test/"),
                )
                .await
                .expect("a cached denial returns immediately")
                .is_err()
            });
        }
        while let Some(result) = tasks.join_next().await {
            assert!(result.unwrap());
        }
        assert_eq!(budget.0.load(Ordering::SeqCst), 1);
        tokio::time::advance(Duration::from_secs(240)).await;
        assert!(vtxo_request_permit(endpoint).await.is_err());
        assert_eq!(budget.0.load(Ordering::SeqCst), 2);
    }

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
            let _permit = vtxo_request_permit(endpoint).await.unwrap();
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
            .unwrap()
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
