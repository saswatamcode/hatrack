use crate::util::error::BoxError;
use hyper::Uri;
use url::Url;

#[derive(Clone, Debug)]
pub struct UpstreamTarget {
    base_scheme_authority: String,
    base_path: String,
}

impl UpstreamTarget {
    pub fn parse_url(s: &str) -> Result<Self, BoxError> {
        let parsed = Url::parse(s)?;

        let scheme = parsed.scheme();
        let authority = parsed.authority();
        let base_path = parsed.path().trim_end_matches('/').to_string();

        Ok(Self {
            base_scheme_authority: format!("{}://{}", scheme, authority),
            base_path,
        })
    }

    pub fn map_request(&self, incoming_uri: &Uri) -> Result<Uri, BoxError> {
        let incoming_path_query = incoming_uri
            .path_and_query()
            .map(|pq| pq.as_str())
            .unwrap_or("/");

        let estimated_capacity =
            self.base_scheme_authority.len() + self.base_path.len() + incoming_path_query.len();

        let mut final_uri_str: String = String::with_capacity(estimated_capacity);

        final_uri_str.push_str(&self.base_scheme_authority);
        final_uri_str.push_str(&self.base_path);
        final_uri_str.push_str(incoming_path_query);

        Ok(Uri::try_from(final_uri_str)?)
    }
}
