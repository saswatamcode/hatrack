use crate::util::error::BoxError;
use std::net::SocketAddr;

pub fn parse_addr(addr: &str) -> Result<SocketAddr, BoxError> {
    let normalized_addr = if addr.starts_with(':') {
        format!("0.0.0.0{}", addr)
    } else {
        addr.to_string()
    };

    normalized_addr.parse::<SocketAddr>().map_err(Into::into)
}
