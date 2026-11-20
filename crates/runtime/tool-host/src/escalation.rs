use std::{future::Future, pin::Pin, sync::Arc};

use nanocode_types_core::responses::{JsonSchema, ToolDefinition};
use serde_json::Value;

use crate::{Tool, ToolContext, ToolInput, ToolResult};

pub const ESCALATION_INPUTS: [&str; 4] = [
    "sandbox_permissions",
    "justification",
    "prefix_rule",
    "dangerouslyDisableSandbox",
];

pub(crate) struct Unescalated {
    inner: Arc<dyn Tool>,
}

impl Unescalated {
    pub(crate) fn wrap(inner: Arc<dyn Tool>) -> Arc<dyn Tool> {
        if declares_escalation(&inner.definition()) {
            Arc::new(Self { inner })
        } else {
            inner
        }
    }
}

fn declares_escalation(definition: &ToolDefinition) -> bool {
    let ToolDefinition::Function { parameters, .. } = definition else {
        return false;
    };
    parameters
        .as_value()
        .get("properties")
        .and_then(Value::as_object)
        .is_some_and(|properties| {
            ESCALATION_INPUTS
                .iter()
                .any(|field| properties.contains_key(*field))
        })
}

fn without_escalation(definition: ToolDefinition) -> ToolDefinition {
    if crate::sim::mutated(crate::sim::Mutation::CatalogKeepsEscalation) {
        return definition;
    }
    let ToolDefinition::Function {
        name,
        description,
        strict,
        defer_loading,
        asynchronous,
        parameters,
        output_schema,
    } = definition
    else {
        return definition;
    };
    let mut schema = parameters.as_value().clone();
    if let Some(properties) = schema.get_mut("properties").and_then(Value::as_object_mut) {
        for field in ESCALATION_INPUTS {
            properties.remove(field);
        }
    }
    if let Some(required) = schema.get_mut("required").and_then(Value::as_array_mut) {
        required.retain(|field| {
            field
                .as_str()
                .is_none_or(|field| !ESCALATION_INPUTS.contains(&field))
        });
    }
    ToolDefinition::Function {
        name,
        description,
        strict,
        defer_loading,
        asynchronous,
        parameters: JsonSchema::from(schema),
        output_schema,
    }
}

fn strip_input(input: ToolInput) -> ToolInput {
    if crate::sim::mutated(crate::sim::Mutation::CallKeepsEscalation) {
        return input;
    }
    let ToolInput::Function(arguments) = &input else {
        return input;
    };
    let Ok(Value::Object(mut fields)) = serde_json::from_str::<Value>(arguments.get()) else {
        return input;
    };
    let before = fields.len();
    for field in ESCALATION_INPUTS {
        fields.remove(field);
    }
    if fields.len() == before {
        return input;
    }
    serde_json::value::to_raw_value(&fields).map_or(input, ToolInput::Function)
}

impl Tool for Unescalated {
    fn definition(&self) -> ToolDefinition {
        without_escalation(self.inner.definition())
    }

    fn supports_parallel_tool_calls(&self) -> bool {
        self.inner.supports_parallel_tool_calls()
    }

    fn running_tasks(&self) -> Vec<nanocode_tools::RunningTask> {
        self.inner.running_tasks()
    }

    fn end_turn<'life0, 'life1, 'life2, 'life3, 'async_trait>(
        &'life0 self,
        session_id: &'life1 str,
        turn_id: &'life2 str,
        hook_event_name: &'life3 str,
    ) -> Pin<Box<dyn Future<Output = Result<(), nanocode_tools::ToolError>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        'life3: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.end_turn(session_id, turn_id, hook_event_name)
    }

    fn execute<'life0, 'life1, 'async_trait>(
        &'life0 self,
        input: ToolInput,
        context: ToolContext<'life1>,
    ) -> Pin<Box<dyn Future<Output = ToolResult> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.execute(strip_input(input), context)
    }
}
