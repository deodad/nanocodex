use nanocode_tool_contracts::{
    Recovered, ToolContext, ToolOutput, ToolOutputContent, ToolProcessTraceWire,
};
use std::sync::Arc;

use serde_json::Value;

use crate::embedded::{HostFuture, NestedToolCall};

pub struct CodeModeToolObservation<'a> {
    pub call: &'a NestedToolCall,
    pub process_trace: Option<&'a ToolProcessTraceWire>,
}

pub trait CodeModeToolset: Send + Sync {
    fn nested_tool_metadata(&self) -> Vec<Value>;

    fn shared_nested_tool_metadata(&self) -> Arc<[Value]> {
        self.nested_tool_metadata().into()
    }

    fn execute_nested<'a>(
        &'a self,
        name: &'a str,
        input: Value,
        context: ToolContext<'a>,
    ) -> HostFuture<'a, ToolOutput>;

    fn execute_host_nested<'a>(
        &'a self,
        name: &'a str,
        input: Value,
        context: ToolContext<'a>,
    ) -> HostFuture<'a, ToolOutput> {
        self.execute_nested(name, input, context)
    }

    fn recover_nested<'a>(
        &'a self,
        _name: &'a str,
        _input: Value,
        _context: ToolContext<'a>,
    ) -> Option<HostFuture<'a, Recovered>> {
        None
    }

    fn cell_observed(&self, _origin_call_id: &str, _observed: bool) {}

    fn append_observation_content(
        &self,
        _calls: &[CodeModeToolObservation<'_>],
        _content: &mut Vec<ToolOutputContent>,
    ) {
    }

    fn session_notices(&self, _session_id: &str) -> Vec<String> {
        Vec::new()
    }

    fn running_output<'a>(&'a self, _call_id: &'a str) -> HostFuture<'a, Option<String>> {
        Box::pin(std::future::ready(None))
    }

    fn returns_within(&self, _name: &str, _input: &Value) -> Option<std::time::Duration> {
        None
    }

    fn session_notice<'a>(&'a self, _session_id: &'a str) -> HostFuture<'a, ()> {
        Box::pin(std::future::pending())
    }

    fn session_busy(&self, _session_id: &str) -> bool {
        false
    }
}
