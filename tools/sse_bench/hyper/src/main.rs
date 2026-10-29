// hyper arm of the SSE/mixed benchmark.
//
// Same contract as tools/sse_bench/server.go: GET / is the 13-byte oneshot
// body, GET /sse is a long-lived event stream of `data: <unix-nanos>\n\n`
// at a ticker interval (sleep to a deadline, not for a duration). GET
// /sse-broadcast is the same stream fed by ONE ticker for every stream, as
// starh2's bench server does it (examples/bench_server.zig). TLS +
// HTTP/2 ALPN, same testdata cert as the other arms. Not the Datastar SDK:
// the client subtracts the stamped nanos, and a patchElements payload would
// be a different measurement.
//
// Why hyper and not axum or actix-web: axum is a router on hyper and adds no
// HTTP/2 work; actix-web has its own HTTP/2 front on the same `h2` crate. The
// h2 frame engine under test is the one that every mainstream Rust server
// uses. Runtime is tokio multi-thread with its default worker count (one per
// logical CPU); TOKIO_WORKER_THREADS=N pins it, the same lever as GOMAXPROCS.
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use http_body_util::{Either, Full};
use hyper::body::{Body, Frame};
use hyper::server::conn::http2;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::{TokioExecutor, TokioIo};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::time::{Interval, MissedTickBehavior};
use tokio_rustls::TlsAcceptor;

struct Args {
    port: u16,
    interval_ms: u64,
    cert: String,
    key: String,
}

fn parse_args() -> Args {
    let mut args = Args {
        port: 8444,
        interval_ms: 100,
        cert: "testdata/cert.pem".to_string(),
        key: "testdata/key.pem".to_string(),
    };
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < argv.len() {
        let flag = argv[i].trim_start_matches('-');
        let value = argv
            .get(i + 1)
            .unwrap_or_else(|| panic!("missing value for -{flag}"));
        match flag {
            "port" => args.port = value.parse().expect("port"),
            "sse-interval-ms" => args.interval_ms = value.parse().expect("sse-interval-ms"),
            "cert" => args.cert = value.clone(),
            "key" => args.key = value.clone(),
            _ => panic!("unknown flag {}", argv[i]),
        }
        i += 2;
    }
    args
}

fn unix_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_nanos()
}

// A body that yields one `data: <nanos>\n\n` frame per tick, forever. hyper
// polls it only when the h2 stream has window and the connection can write,
// so a slow reader parks this body and nothing else. Dropped on reset.
struct TickerBody {
    interval: Interval,
}

impl Body for TickerBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        match self.interval.poll_tick(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(_) => {
                let line = format!("data: {}\n\n", unix_nanos());
                Poll::Ready(Some(Ok(Frame::data(Bytes::from(line)))))
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        false
    }
}

// The broadcast stream: every body waits on one watch channel that a single
// ticker task stamps, so one timer serves all streams. A body that misses a
// tick sends the latest stamp once, as starh2's condition-variable arm does.
// The receiver moves into the pending future and comes back with its result,
// because `changed()` borrows it.
type Changed = Pin<Box<dyn Future<Output = (watch::Receiver<u128>, bool)> + Send>>;

struct BroadcastBody {
    changed: Changed,
}

fn wait_changed(mut rx: watch::Receiver<u128>) -> Changed {
    Box::pin(async move {
        let ok = rx.changed().await.is_ok();
        (rx, ok)
    })
}

impl Body for BroadcastBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        match self.changed.as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready((_, false)) => Poll::Ready(None),
            Poll::Ready((mut rx, true)) => {
                let ts = *rx.borrow_and_update();
                self.changed = wait_changed(rx);
                let line = format!("data: {}\n\n", ts);
                Poll::Ready(Some(Ok(Frame::data(Bytes::from(line)))))
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        false
    }
}

type ArmBody = Either<Full<Bytes>, Either<TickerBody, BroadcastBody>>;

fn handle(
    req: Request<hyper::body::Incoming>,
    interval: Duration,
    ticks: watch::Receiver<u128>,
) -> impl Future<Output = Result<Response<ArmBody>, hyper::Error>> {
    let path = req.uri().path().to_string();
    async move {
        if path == "/sse-broadcast" {
            let mut rx = ticks;
            // Wait for the next tick, not the one already stamped.
            rx.borrow_and_update();
            let resp = Response::builder()
                .status(200)
                .header("content-type", "text/event-stream")
                .header("cache-control", "no-cache")
                .body(Either::Right(Either::Right(BroadcastBody { changed: wait_changed(rx) })))
                .expect("static response");
            return Ok(resp);
        }
        if path == "/sse" {
            // Interval's first tick is immediate; the Go arm waits one period
            // before its first event. Skip the immediate tick to match.
            let mut ticker = tokio::time::interval_at(
                tokio::time::Instant::now() + interval,
                interval,
            );
            // Go's Ticker drops ticks for a slow receiver; Kestrel's
            // PeriodicTimer does the same. Skip matches that, Burst would
            // emit a catch-up burst that neither opponent emits.
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let resp = Response::builder()
                .status(200)
                .header("content-type", "text/event-stream")
                .header("cache-control", "no-cache")
                .body(Either::Right(Either::Left(TickerBody { interval: ticker })))
                .expect("static response");
            return Ok(resp);
        }
        let resp = Response::builder()
            .status(200)
            .header("content-type", "text/plain")
            .body(Either::Left(Full::new(Bytes::from_static(b"Hello, World!"))))
            .expect("static response");
        Ok(resp)
    }
}

fn load_tls(cert_path: &str, key_path: &str) -> Arc<rustls::ServerConfig> {
    let cert_file = std::fs::File::open(cert_path)
        .unwrap_or_else(|e| panic!("open {cert_path}: {e}"));
    let certs: Vec<CertificateDer<'static>> =
        rustls_pemfile::certs(&mut std::io::BufReader::new(cert_file))
            .collect::<Result<_, _>>()
            .expect("parse certificate chain");
    let key_file =
        std::fs::File::open(key_path).unwrap_or_else(|e| panic!("open {key_path}: {e}"));
    let key: PrivateKeyDer<'static> =
        rustls_pemfile::private_key(&mut std::io::BufReader::new(key_file))
            .expect("parse private key")
            .expect("no private key in file");
    let mut config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .expect("server config");
    // h2 only. The other arms also refuse HTTP/1.1 on this port, so a client
    // that fails ALPN fails here too instead of silently measuring h1.
    config.alpn_protocols = vec![b"h2".to_vec()];
    Arc::new(config)
}

#[tokio::main]
async fn main() {
    let args = parse_args();
    let interval = Duration::from_millis(args.interval_ms);
    let tls = TlsAcceptor::from(load_tls(&args.cert, &args.key));
    let addr: SocketAddr = ([127, 0, 0, 1], args.port).into();
    let listener = TcpListener::bind(addr)
        .await
        .unwrap_or_else(|e| panic!("bind {addr}: {e}"));
    // width is the scheduler width this arm actually runs with, so the
    // harness can check that every arm is pinned the same
    // (TOKIO_WORKER_THREADS).
    let width = tokio::runtime::Handle::current().metrics().num_workers();
    let (tick_tx, tick_rx) = watch::channel(0u128);
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            tick_tx.send_replace(unix_nanos());
        }
    });
    println!("{{\"ready\":true,\"port\":{},\"width\":{}}}", args.port, width);

    loop {
        let (tcp, _) = match listener.accept().await {
            Ok(pair) => pair,
            Err(e) => {
                eprintln!("accept: {e}");
                continue;
            }
        };
        // tokio does not set TCP_NODELAY on accepted sockets; Go's net/http
        // and Kestrel do. Without it the Linux delayed-ACK timer puts a
        // 40ms ceiling on small SSE writes and the arm reads p99=40ms while
        // the same binary reads microseconds on macOS (nachos run, b68a356).
        if let Err(e) = tcp.set_nodelay(true) {
            eprintln!("set_nodelay: {e}");
            continue;
        }
        let tls = tls.clone();
        let tick_rx = tick_rx.clone();
        tokio::spawn(async move {
            let stream = match tls.accept(tcp).await {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("tls accept: {e}");
                    return;
                }
            };
            let mut builder = http2::Builder::new(TokioExecutor::new());
            // hyper's default is 200, the same cap Kestrel raises from 100:
            // run.sh opens 200 streams and then probes on the same socket.
            builder.max_concurrent_streams(1024);
            let ticks = tick_rx.clone();
            let svc = service_fn(move |req| handle(req, interval, ticks.clone()));
            if let Err(e) = builder.serve_connection(TokioIo::new(stream), svc).await {
                // A client that closes mid-stream is the normal end of a round.
                let _ = e;
            }
        });
    }
}
