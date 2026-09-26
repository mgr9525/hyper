#![deny(warnings)]

use std::{future::Future, pin::Pin, time::Duration};

use bytes::Bytes;
use futures_util::future;
use hyper::{client::conn, Request, Uri};

#[path = "support/tokiort.rs"]
mod tokiort;
use tokiort::{TokioExecutor, TokioIo};

#[tokio::test]
async fn http2_try_send_request_returns_head_not_written() {
    use hyper::body::{Body, Frame, SizeHint};
    use std::convert::Infallible;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::task::{Context, Poll};
    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

    #[derive(Clone)]
    struct Marker;

    struct CountingBody {
        polls: Arc<AtomicUsize>,
    }

    impl Body for CountingBody {
        type Data = Bytes;
        type Error = Infallible;

        fn poll_frame(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            self.get_mut().polls.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(None)
        }

        fn is_end_stream(&self) -> bool {
            false
        }

        fn size_hint(&self) -> SizeHint {
            SizeHint::default()
        }
    }

    struct FailOnRequestHeaders {
        written: Vec<u8>,
        saw_request_headers: Arc<AtomicBool>,
    }

    impl FailOnRequestHeaders {
        fn request_headers_offset(bytes: &[u8]) -> Option<usize> {
            const PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

            let mut offset = if bytes.starts_with(PREFACE) {
                PREFACE.len()
            } else {
                0
            };

            while bytes.len().saturating_sub(offset) >= 4 {
                // The frame type is available before the complete frame
                // header. Stop before accepting any request HEADERS bytes.
                if bytes[offset + 3] == 0x1 {
                    return Some(offset);
                }

                if bytes.len().saturating_sub(offset) < 9 {
                    break;
                }

                let len = (usize::from(bytes[offset]) << 16)
                    | (usize::from(bytes[offset + 1]) << 8)
                    | usize::from(bytes[offset + 2]);
                let next = match offset.checked_add(9 + len) {
                    Some(next) => next,
                    None => break,
                };
                if next > bytes.len() {
                    break;
                }
                offset = next;
            }

            None
        }
    }

    impl AsyncRead for FailOnRequestHeaders {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Pending
        }
    }

    impl AsyncWrite for FailOnRequestHeaders {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            let written = self.written.len();
            let mut candidate = self.written.clone();
            candidate.extend_from_slice(buf);

            if let Some(offset) = Self::request_headers_offset(&candidate) {
                self.saw_request_headers.store(true, Ordering::SeqCst);
                let accepted = offset.saturating_sub(written);
                if accepted != 0 {
                    self.written.extend_from_slice(&buf[..accepted]);
                    return Poll::Ready(Ok(accepted));
                }

                return Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "request HEADERS write failed",
                )));
            }

            self.written.extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    let polls = Arc::new(AtomicUsize::new(0));
    let saw_request_headers = Arc::new(AtomicBool::new(false));
    let (mut client, conn) = conn::http2::Builder::new(TokioExecutor)
        .handshake::<_, CountingBody>(TokioIo::new(FailOnRequestHeaders {
            written: Vec::new(),
            saw_request_headers: saw_request_headers.clone(),
        }))
        .await
        .expect("http2 handshake");

    let mut request = Request::builder()
        .uri("https://example.com/retry")
        .header("x-request", "original")
        .body(CountingBody {
            polls: polls.clone(),
        })
        .unwrap();
    request.extensions_mut().insert(Marker);

    let mut response = std::pin::pin!(client.try_send_request(request));
    future::poll_fn(|cx| {
        assert!(response.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;

    tokio::spawn(async move {
        let _ = conn.await;
    });

    let error = tokio::time::timeout(Duration::from_secs(2), response.as_mut())
        .await
        .expect("request should complete after write failure")
        .expect_err("request must fail");
    assert!(saw_request_headers.load(Ordering::SeqCst));
    let returned = error.message().expect("request should be returned");
    assert_eq!(
        returned.uri(),
        &Uri::from_static("https://example.com/retry")
    );
    assert_eq!(returned.headers()["x-request"], "original");
    assert!(returned.extensions().get::<Marker>().is_some());
    assert_eq!(
        polls.load(Ordering::SeqCst),
        0,
        "body was polled before HEADERS"
    );
}
