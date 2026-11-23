use axum::{
    body::{Body, Bytes},
    extract::{Request, State},
    middleware::Next,
    response::Response,
};
use fleet_runtime::{DueBatchYield, ReplyDueWork, ReplyDueWorkOwner, collect_reply_due_work};
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use crate::TokioWorkspaceHost;

impl ReplyDueWorkOwner for TokioWorkspaceHost {
    fn run_after_reply(&self, work: ReplyDueWork) {
        self.background_tasks.schedule(Box::pin(async move {
            tokio::task::yield_now().await;
            work.run().await;
        }));
    }

    fn yield_due_batches(&self) -> DueBatchYield {
        fleet_runtime::yield_shared_due_batches()
    }
}

pub(super) async fn handoff_signin_reply(
    State(host): State<Arc<TokioWorkspaceHost>>,
    request: Request,
    next: Next,
) -> Response {
    let (reply, work) = collect_reply_due_work(next.run(request)).await;
    if work.is_empty() {
        return reply;
    }
    let (parts, body) = reply.into_parts();
    Response::from_parts(
        parts,
        Body::new(SignInReplyBody {
            body,
            handoff: Some((host, work)),
        }),
    )
}

/// Hyper reaches the response body after the handler returns. Hand off on its
/// final frame, preserving the original response data and any trailers.
struct SignInReplyBody {
    body: Body,
    handoff: Option<(Arc<TokioWorkspaceHost>, ReplyDueWork)>,
}

impl http_body::Body for SignInReplyBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        let frame = Pin::new(&mut this.body).poll_frame(context);
        if (matches!(frame, Poll::Ready(None))
            || matches!(frame, Poll::Ready(Some(Ok(_)))) && this.body.is_end_stream())
            && let Some((host, work)) = this.handoff.take()
        {
            work.handoff(host.as_ref());
        }
        frame
    }
}
