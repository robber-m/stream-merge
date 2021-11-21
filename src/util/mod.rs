use core::pin::Pin;
use futures::future::Future;
use futures::stream::{Fuse, FuturesOrdered, Stream, StreamExt};
use futures::task::{Context, Poll};

use pin_project_lite::pin_project;

pin_project! {
    /// [Stream] combinator structure which applies the same buffering scheme as [futures::stream::Buffered],
    /// but waits to spawn any concurrent futures (i.e. push new futures into the [FuturesOrdered] `in_progress_queue`)
    /// until `take_n_serially` requests have been returned as [Poll::Ready].
    ///
    /// This struct is used in a larger project which downloads files from AWS S3 merges them together in time-sequence.
    /// By delaying concurrent file chunk requests when wrapping [super::s3::ObjectChunks], we can avoid spending memory
    /// and cpu time buffering [Bytes] for files which will not be needed until much later in time-sequence (e.g. the 11pm
    /// pcap when merging hourly-rolled .pcap files from 1am that morning). Once we have downloaded and decompressed (if necessary)
    /// enough bytes to read the timestamp associated with the first packet in all .pcap files, merging proceeds in time order,
    /// so we will not need the buffered data for end-of-sequence pcap files and we will not continue to pull out more packets for
    /// that file until that point. The effect is that buffering and concurrency are delayed for a given pcap until `take_n_serially`
    /// items have been serially evaluated by the stream, which does not occur until that file becomes "active" in the playback.
    #[derive(Debug)]
    #[must_use = "streams do nothing unless polled"]
    pub struct TakeThenBuffered<St>
    where
        St: Stream,
        St::Item: Future,
    {
        #[pin]
        stream: Fuse<St>,
        in_progress_queue: FuturesOrdered<St::Item>,
        take_n_serially: usize,
        max_n_buffered: usize,
    }
}

impl<St> TakeThenBuffered<St>
where
    St: Stream,
    St::Item: Future,
{
    pub(super) fn new(stream: St, take_n_serially: usize, max_n_buffered: usize) -> Self {
        Self {
            stream: stream.fuse(),
            in_progress_queue: FuturesOrdered::new(),
            take_n_serially,
            max_n_buffered,
        }
    }
}

impl<St> Stream for TakeThenBuffered<St>
where
    St: Stream,
    St::Item: Future,
{
    type Item = <St::Item as Future>::Output;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        // First up, try to spawn off as many futures as possible by filling up
        // our queue of futures. Propagate errors from the stream immediately.
        while this.in_progress_queue.len()
            < (if *this.take_n_serially > 0 {
                1
            } else {
                *this.max_n_buffered
            })
        {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(fut)) => {
                    this.in_progress_queue.push(fut);
                    *this.take_n_serially = this.take_n_serially.saturating_sub(1);
                }
                Poll::Ready(None) | Poll::Pending => break,
            }
        }

        // Attempt to pull the next value from the in_progress_queue
        match this.in_progress_queue.poll_next_unpin(cx) {
            Poll::Pending => return Poll::Pending,
            x @ Poll::Ready(Some(_)) => return x,
            Poll::Ready(None) => {}
        }

        // If more values are still coming from the stream, we're not done yet
        if this.stream.is_done() {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    #[test]
    fn test_initial_n_tasks_are_spawned_serially_then_m_are_buffered_in_parallel() {
        let mut stream = TakeThenBuffered::new(
            futures::stream::iter(
                vec![1, 2, 3, 4, 5, 6, 7, 8]
                    .into_iter()
                    .map(futures::future::ready),
            ),
            3,
            4,
        );

        futures_test::assert_stream_next!(stream, 1);
        assert_eq!(stream.in_progress_queue.len(), 0);

        futures_test::assert_stream_next!(stream, 2);
        assert_eq!(stream.in_progress_queue.len(), 0);

        futures_test::assert_stream_next!(stream, 3);
        assert_eq!(stream.in_progress_queue.len(), 3); // the next three values [4, 5, 6] should be buffered in the queue
                                                       // NOTE: we expect there to be three buffered items in the queue
                                                       // instead of four because the next future [3] is immediately Poll::Ready(3)
                                                       // and is returned in that same poll_next() call.

        futures_test::assert_stream_next!(stream, 4);
        assert_eq!(stream.in_progress_queue.len(), 3); // the next three values [5, 6, 7] should be buffered in the queue

        futures_test::assert_stream_next!(stream, 5);
        assert_eq!(stream.in_progress_queue.len(), 3); // the next three values [6, 7, 8] should be buffered in the queue

        futures_test::assert_stream_next!(stream, 6);
        assert_eq!(stream.in_progress_queue.len(), 2); // the next two   values [7, 8]    should be buffered in the queue

        futures_test::assert_stream_next!(stream, 7);
        assert_eq!(stream.in_progress_queue.len(), 1); // the next one   value  [8]       should be buffered in the queue

        futures_test::assert_stream_next!(stream, 8);
        assert_eq!(stream.in_progress_queue.len(), 0);

        futures_test::assert_stream_done!(stream);
    }
}
