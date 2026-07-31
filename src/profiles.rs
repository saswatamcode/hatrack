use axum::{
    extract::Query,
    http::{StatusCode, header},
    response::IntoResponse,
};
use pprof::protos::Message;
use serde::Deserialize;
use std::time::Duration;

#[derive(Deserialize)]
pub struct ProfileParams {
    seconds: Option<u64>,
}

// Returns raw pprof-format protobuf
pub async fn handle_pprof_profile(
    Query(params): Query<ProfileParams>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let seconds = params.seconds.unwrap_or(10);

    let guard = pprof::ProfilerGuardBuilder::default()
        .frequency(1000)
        .build()
        .map_err(|err| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Profiler init failed: {err}"),
            )
        })?;

    tokio::time::sleep(Duration::from_secs(seconds)).await;

    let report = guard.report().build().map_err(|err| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Profile report build failed: {err}"),
        )
    })?;

    let profile = report.pprof().map_err(|err| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("pprof generation failed: {err}"),
        )
    })?;

    let mut body = Vec::new();
    profile.write_to_vec(&mut body).map_err(|err| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Profile encoding failed: {err}"),
        )
    })?;

    let headers = [(header::CONTENT_TYPE, "application/octet-stream")];
    Ok((headers, body))
}

// Referenced from https://github.com/polarsignals/rust-jemalloc-pprof
pub async fn handle_get_heap() -> Result<impl IntoResponse, (StatusCode, String)> {
    let mut prof_ctl = jemalloc_pprof::PROF_CTL.as_ref().unwrap().lock().await;
    require_profiling_activated(&prof_ctl)?;
    let pprof = prof_ctl
        .dump_pprof()
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
    Ok(pprof)
}

// Referenced from https://github.com/polarsignals/rust-jemalloc-pprof
/// Checks whether jemalloc profiling is activated an returns an error response if not.
fn require_profiling_activated(
    prof_ctl: &jemalloc_pprof::JemallocProfCtl,
) -> Result<(), (axum::http::StatusCode, String)> {
    if prof_ctl.activated() {
        Ok(())
    } else {
        Err((
            axum::http::StatusCode::FORBIDDEN,
            "heap profiling not activated".into(),
        ))
    }
}
