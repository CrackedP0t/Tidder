use common::CONFIG;
use core::fmt;
use core::pin::Pin;
use futures::future::Future;
use futures::stream::{Fuse, FuturesUnordered, StreamExt};
use futures::stream::{FusedStream, Stream};
use futures::task::{Context, Poll};
use pin_project_lite::pin_project;

pub fn is_limited() -> bool {
    let now = chrono::Local::now().time();
    now > CONFIG.time_limits.start && now < CONFIG.time_limits.end
}

pin_project! {
    /// Stream for the [`buffer_unordered`](super::StreamExt::buffer_unordered)
    /// method.
    #[must_use = "streams do nothing unless polled"]
    pub struct BufferUnordered<St>
    where
        St: Stream,
    {
        #[pin]
        stream: Fuse<St>,
        in_progress_queue: FuturesUnordered<St::Item>,
    }
}

impl<St> fmt::Debug for BufferUnordered<St>
where
    St: Stream + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BufferUnordered")
            .field("stream", &self.stream)
            .field("in_progress_queue", &self.in_progress_queue)
            .finish()
    }
}

impl<St> BufferUnordered<St>
where
    St: Stream,
    St::Item: Future,
{
    pub(super) fn new(stream: St) -> Self
    where
        St: Stream,
        St::Item: Future,
    {
        Self {
            stream: stream.fuse(),
            in_progress_queue: FuturesUnordered::new(),
        }
    }
}

impl<St> Stream for BufferUnordered<St>
where
    St: Stream,
    St::Item: Future,
{
    type Item = <St::Item as Future>::Output;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        let max = if is_limited() {
            CONFIG.time_limits.count
        } else {
            CONFIG.worker_count
        };

        // First up, try to spawn off as many futures as possible by filling up
        // our queue of futures.
        while this.in_progress_queue.len() < max {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(fut)) => this.in_progress_queue.push(fut),
                Poll::Ready(None) | Poll::Pending => break,
            }
        }

        // Attempt to pull the next value from the in_progress_queue
        match this.in_progress_queue.poll_next_unpin(cx) {
            x @ Poll::Pending | x @ Poll::Ready(Some(_)) => return x,
            Poll::Ready(None) => {}
        }

        // If more values are still coming from the stream, we're not done yet
        if this.stream.is_done() {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let queue_len = self.in_progress_queue.len();
        let (lower, upper) = self.stream.size_hint();
        let lower = lower.saturating_add(queue_len);
        let upper = match upper {
            Some(x) => x.checked_add(queue_len),
            None => None,
        };
        (lower, upper)
    }
}

impl<St> FusedStream for BufferUnordered<St>
where
    St: Stream,
    St::Item: Future,
{
    fn is_terminated(&self) -> bool {
        self.in_progress_queue.is_terminated() && self.stream.is_terminated()
    }
}
