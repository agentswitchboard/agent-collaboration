//! A cancellable platform turn, available to both reply tasks and alarm passes.
use fleet_runtime::{DueBatchYield, DueBatchYieldFailure};
use js_sys::{Function, Object, Promise, Reflect, futures::JsFuture};
use web_sys::AbortController;
use worker::send::SendFuture;
use worker::wasm_bindgen::{JsCast, JsValue};

pub(super) fn yield_worker_due_batches() -> DueBatchYield {
    Box::pin(SendFuture::new(async {
        let controller = AbortController::new().map_err(|_| DueBatchYieldFailure)?;
        let cancellation = PendingYield(Some(controller));
        let options = Object::new();
        let signal = cancellation
            .0
            .as_ref()
            .ok_or(DueBatchYieldFailure)?
            .signal();
        Reflect::set(&options, &JsValue::from_str("signal"), signal.as_ref())
            .map_err(|_| DueBatchYieldFailure)?;
        #[cfg(feature = "test-support")]
        worker::console_log!("Central due yield scheduled");
        let scheduler = Reflect::get(&js_sys::global(), &JsValue::from_str("scheduler"))
            .map_err(|_| DueBatchYieldFailure)?;
        let wait = Reflect::get(&scheduler, &JsValue::from_str("wait"))
            .and_then(JsCast::dyn_into::<Function>)
            .map_err(|_| DueBatchYieldFailure)?;
        let promise = wait
            .call2(&scheduler, &JsValue::from(1), options.as_ref())
            .and_then(JsCast::dyn_into::<Promise>)
            .map_err(|_| DueBatchYieldFailure)?;
        let result = JsFuture::from(promise).await;
        cancellation.settle();
        #[cfg(feature = "test-support")]
        worker::console_log!("Central due yield settled");
        result.map(|_| ()).map_err(|_| DueBatchYieldFailure)
    }))
}

struct PendingYield(Option<AbortController>);

impl PendingYield {
    fn settle(mut self) {
        self.0.take();
    }
}

impl Drop for PendingYield {
    fn drop(&mut self) {
        if let Some(controller) = self.0.take() {
            controller.abort();
        }
    }
}
