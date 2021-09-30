#[cfg(test)]
mod tests;

pub mod agent;
pub mod logs;
pub mod metrics;
pub mod traces;

use crate::config::SourceDescription;
use crate::sources::datadog::agent::DatadogAgentConfig;

inventory::submit! {
    SourceDescription::new::<DatadogAgentConfig>("datadog_agent")
}
