use crate::{
    event::{Event, LogEvent, Value},
    internal_events::EventsReceived,
    sources::datadog::agent::{self, handle_request, ApiKeyQueryParams, DatadogAgentSource},
    sources::util::ErrorMessage,
    vector_core::ByteSizeOf,
    SourceSender,
};
use bytes::Bytes;
use chrono::{TimeZone, Utc};
use futures::future;
use http::StatusCode;
use prost::Message;
use std::collections::BTreeMap;
use std::sync::Arc;
use warp::{filters::BoxedFilter, path, path::FullPath, reply::Response, Filter, Rejection, Reply};

mod dd_proto {
    include!(concat!(env!("OUT_DIR"), "/pb.rs"));
}

pub(crate) fn build_warp_filter(
    acknowledgements: bool,
    multiple_outputs: bool,
    out: SourceSender,
    source: DatadogAgentSource,
) -> BoxedFilter<(Response,)> {
    build_trace_filter(acknowledgements, multiple_outputs, out, source)
        .or(build_stats_filter())
        .unify()
        .boxed()
}

fn build_trace_filter(
    acknowledgements: bool,
    multiple_outputs: bool,
    out: SourceSender,
    source: DatadogAgentSource,
) -> BoxedFilter<(Response,)> {
    warp::post()
        .and(path!("api" / "v0.2" / "traces" / ..))
        .and(warp::path::full())
        .and(warp::header::optional::<String>("content-encoding"))
        .and(warp::header::optional::<String>("dd-api-key"))
        .and(warp::header::optional::<String>(
            "X-Datadog-Reported-Languages",
        ))
        .and(warp::query::<ApiKeyQueryParams>())
        .and(warp::body::bytes())
        .and_then(
            move |path: FullPath,
                  encoding_header: Option<String>,
                  api_token: Option<String>,
                  reported_language: Option<String>,
                  query_params: ApiKeyQueryParams,
                  body: Bytes| {
                let events = source
                    .decode(&encoding_header, body, path.as_str())
                    .and_then(|body| {
                        decode_dd_trace_payload(
                            body,
                            source.api_key_extractor.extract(
                                path.as_str(),
                                api_token,
                                query_params.dd_api_key,
                            ),
                            reported_language.as_ref(),
                            &source,
                        )
                        .map_err(|error| {
                            ErrorMessage::new(
                                StatusCode::UNPROCESSABLE_ENTITY,
                                format!("Error decoding Datadog traces: {:?}", error),
                            )
                        })
                    });
                if multiple_outputs {
                    handle_request(events, acknowledgements, out.clone(), Some(agent::LOGS))
                } else {
                    handle_request(events, acknowledgements, out.clone(), None)
                }
            },
        )
        .boxed()
}

fn build_stats_filter() -> BoxedFilter<(Response,)> {
    warp::post()
        .and(path!("api" / "v0.2" / "stats" / ..))
        .and_then(|| {
            warn!(message = "/api/v0.2/stats route is yet not supported.");
            let response: Result<Response, Rejection> = Ok(warp::reply().into_response());
            future::ready(response)
        })
        .boxed()
}

fn decode_dd_trace_payload(
    frame: Bytes,
    api_key: Option<Arc<str>>,
    lang: Option<&String>,
    source: &DatadogAgentSource,
) -> crate::Result<Vec<Event>> {
    let decoded_payload = dd_proto::TracePayload::decode(frame)?;
    let env = decoded_payload.env;
    let hostname = decoded_payload.host_name;
    // Each traces is map to one event...
    let trace_events: Vec<Event> = decoded_payload
        .traces
        .iter()
        .map(|dd_traces| into_event(dd_traces, env.clone(), hostname.clone(), source))
        //... and each APM event is also map into its own event
        .chain(decoded_payload.transactions.iter().map(|s| {
            let mut log_event = LogEvent::from(convert_span(s));
            log_event.insert(
                source.log_schema_source_type_key,
                Bytes::from("datadog_agent"),
            );
            log_event.insert(source.log_schema_host_key, hostname.clone());
            log_event.insert("env", env.clone());
            log_event
        }))
        .map(|mut log_event| {
            if let Some(k) = &api_key {
                log_event
                    .metadata_mut()
                    .set_datadog_api_key(Some(Arc::clone(k)));
            }
            if let Some(lang) = lang {
                log_event.insert("language", lang.clone());
            }
            log_event.into()
        })
        .collect();

    emit!(&EventsReceived {
        byte_size: trace_events.size_of(),
        count: trace_events.len(),
    });

    Ok(trace_events)
}

fn into_event(
    dd_trace: &dd_proto::ApiTrace,
    env: String,
    hostname: String,
    source: &DatadogAgentSource,
) -> LogEvent {
    let mut log_event = LogEvent::default();
    log_event.insert(
        source.log_schema_source_type_key,
        Bytes::from("datadog_agent"),
    );
    log_event.insert(source.log_schema_host_key, hostname);
    log_event.insert("env", env);

    log_event.insert("trace_id", dd_trace.trace_id as i64);
    log_event.insert("start_time", Utc.timestamp_nanos(dd_trace.start_time));
    log_event.insert("end_time", Utc.timestamp_nanos(dd_trace.end_time));
    log_event.insert(
        "spans",
        dd_trace
            .spans
            .iter()
            .map(|s| Value::from(convert_span(s)))
            .collect::<Vec<Value>>(),
    );
    log_event
}

fn convert_span(dd_span: &dd_proto::Span) -> BTreeMap<String, Value> {
    let mut span = BTreeMap::<String, Value>::new();
    span.insert("service".into(), Value::from(dd_span.service.clone()));
    span.insert("name".into(), Value::from(dd_span.name.clone()));
    span.insert("resource".into(), Value::from(dd_span.resource.clone()));
    span.insert("trace_id".into(), Value::from(dd_span.trace_id as i64));
    span.insert("span_id".into(), Value::from(dd_span.span_id as i64));
    span.insert("parent_id".into(), Value::from(dd_span.parent_id as i64));
    span.insert(
        "start".into(),
        Value::from(Utc.timestamp_nanos(dd_span.start)),
    );
    span.insert("duration".into(), Value::from(dd_span.duration as i64));
    span.insert("error".into(), Value::from(dd_span.error as i64));
    span.insert(
        "meta".into(),
        Value::from(
            dd_span
                .meta
                .iter()
                .map(|(k, v)| (k.clone(), Value::from(v.clone())))
                .collect::<BTreeMap<String, Value>>(),
        ),
    );
    span.insert(
        "metrics".into(),
        Value::from(
            dd_span
                .metrics
                .iter()
                .map(|(k, v)| (k.clone(), Value::from(*v)))
                .collect::<BTreeMap<String, Value>>(),
        ),
    );
    span.insert("type".into(), Value::from(dd_span.r#type.clone()));
    span
}
