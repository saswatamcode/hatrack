use axum::{
    body::Body,
    extract::Request,
    http::{Response, StatusCode},
};
use futures_util::{StreamExt, stream::unfold};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, StreamBody};
use hyper::body::Bytes;
use hyper::body::Frame;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::{Connect, HttpConnector};
use hyper_util::rt::TokioExecutor;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error};

pub type HttpClient = Client<HttpConnector, ProxyBody>;
pub type ProxyBody = BoxBody<Bytes, std::io::Error>;

#[derive(Clone, Debug)]
pub struct HTTPClientConfig {
    /// Cap on time to establish a TCP connection.
    pub connect_timeout: Option<Duration>,
    /// TCP keepalive interval on the underlying socket.
    pub tcp_keepalive: Option<Duration>,
    /// How long an idle pooled connection is kept before being dropped.
    pub pool_idle_timeout: Option<Duration>,
    /// Max idle connections kept per upstream host.
    pub pool_max_idle_per_host: usize,
}

impl Default for HTTPClientConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Some(Duration::from_secs(10)),
            tcp_keepalive: Some(Duration::from_secs(60)),
            pool_idle_timeout: Some(Duration::from_secs(90)),
            pool_max_idle_per_host: 32,
        }
    }
}

impl HTTPClientConfig {
    fn configure_http_connector(&self) -> HttpConnector {
        let mut connector = HttpConnector::new();
        connector.set_connect_timeout(self.connect_timeout);
        connector.set_keepalive(self.tcp_keepalive);
        connector.set_nodelay(true);
        connector.set_reuse_address(true);
        connector
    }

    pub fn build_client_with_connector<C>(
        &self,
        connector: C,
        executor: &TokioExecutor,
    ) -> Client<C, ProxyBody>
    where
        C: Connect + Clone + Send + Sync + 'static,
    {
        let mut builder = Client::builder(executor.clone());
        builder
            .pool_idle_timeout(self.pool_idle_timeout)
            .pool_max_idle_per_host(self.pool_max_idle_per_host);
        builder.build(connector)
    }

    pub fn build_default_client(&self, executor: &TokioExecutor) -> HttpClient {
        debug!("building default http client");
        let connector = self.configure_http_connector();
        self.build_client_with_connector(connector, executor)
    }
}

pub fn streaming_body_from_axum(body: Body) -> ProxyBody {
    let (tx, rx) = mpsc::channel::<Result<Frame<Bytes>, std::io::Error>>(8);

    tokio::spawn(async move {
        let mut data = body.into_data_stream();

        while let Some(chunk) = data.next().await {
            let item = match chunk {
                Ok(bytes) => Ok(Frame::data(bytes)),
                Err(error) => Err(std::io::Error::other(error)),
            };

            if tx.send(item).await.is_err() {
                break;
            }
        }
    });

    let body_stream = unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    });

    BodyExt::boxed(StreamBody::new(body_stream))
}

pub fn empty_response(status: StatusCode) -> Response<Body> {
    Response::builder()
        .status(status)
        .body(Body::empty())
        .unwrap_or_else(|e| {
            error!(error = %e, %status, "failed to build HTTP response");
            Response::new(Body::empty())
        })
}

pub fn header_value<'a>(req: &'a Request, name: &str) -> Option<&'a str> {
    req.headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
}
