use std::fmt::Debug;

use serde::{Serialize, de::DeserializeOwned};
use switchboard_session_payloads::{
    AssistantBody, FirstSeenAtMs, GroupPlace, Item, ItemBody, LineRange, PlanEntry, SessionBody,
    ToolAction, ToolCall, ToolKind, ToolState, UsageAmount, UsageMetric, UsageReading, UsageScope,
    UserBody,
};

use super::assert_wire;

#[derive(Serialize)]
pub(super) struct ItemExample {
    pub name: String,
    pub value: Item,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<&'static str>,
}

#[derive(Default)]
pub(super) struct ItemExamples {
    pub fixtures: Vec<ItemExample>,
}

impl ItemExamples {
    pub(super) fn check<T>(&mut self, name: &str, wire: &str, expected: T)
    where
        T: DeserializeOwned + Serialize + Debug + PartialEq + ItemFixture,
    {
        let items = expected.items();
        assert_wire(wire, expected);
        let indexed = items.len() > 1;
        for (index, item) in items.into_iter().enumerate() {
            let name = if indexed {
                format!("{name}-{}", index + 1)
            } else {
                name.to_owned()
            };
            self.fixtures.push(ItemExample {
                name,
                value: item,
                description: None,
            });
        }
    }

    pub(super) fn add(&mut self, name: &str, value: Item) -> &mut ItemExample {
        self.fixtures.push(ItemExample {
            name: name.to_owned(),
            value,
            description: None,
        });
        self.fixtures.last_mut().unwrap()
    }
}

pub(super) trait ItemFixture {
    fn items(&self) -> Vec<Item>;
}

impl ItemFixture for Item {
    fn items(&self) -> Vec<Item> {
        vec![self.clone()]
    }
}

impl ItemFixture for Vec<Item> {
    fn items(&self) -> Vec<Item> {
        self.clone()
    }
}

impl ItemFixture for AssistantBody {
    fn items(&self) -> Vec<Item> {
        vec![assistant_item(self.clone())]
    }
}

impl ItemFixture for SessionBody {
    fn items(&self) -> Vec<Item> {
        vec![item(ItemBody::Session { body: self.clone() })]
    }
}

impl ItemFixture for PlanEntry {
    fn items(&self) -> Vec<Item> {
        AssistantBody::Plan {
            entries: vec![self.clone()],
        }
        .items()
    }
}

impl ItemFixture for ToolCall {
    fn items(&self) -> Vec<Item> {
        AssistantBody::ToolCall { call: self.clone() }.items()
    }
}

impl ItemFixture for ToolAction {
    fn items(&self) -> Vec<Item> {
        tool_call(self.clone(), ToolState::Pending).items()
    }
}

impl ItemFixture for ToolState {
    fn items(&self) -> Vec<Item> {
        tool_call(ToolAction::NotReported, self.clone()).items()
    }
}

pub(super) fn tool_call(action: ToolAction, state: ToolState) -> ToolCall {
    ToolCall {
        source_name: None,
        kind: ToolKind::Other,
        title: None,
        action,
        state,
        locations: vec![],
    }
}

pub(super) fn item(body: ItemBody) -> Item {
    Item {
        id: "fixture-item".parse().unwrap(),
        turn: Some("fixture-turn".parse().unwrap()),
        order: 0.into(),
        first_seen_at_ms: fixture_time(),
        body,
    }
}

pub(super) fn user_item() -> Item {
    item(ItemBody::User {
        body: UserBody::Message {
            text: "Hello".into(),
        },
    })
}

pub(super) fn assistant_item(body: AssistantBody) -> Item {
    item(ItemBody::Assistant {
        group: GroupPlace {
            id: "fixture-group".parse().unwrap(),
            order: 0.into(),
        },
        body,
    })
}

pub(super) fn tool_item(action: ToolAction, state: ToolState) -> Item {
    assistant_item(AssistantBody::ToolCall {
        call: tool_call(action, state),
    })
}

pub(super) fn file_read_item() -> Item {
    tool_item(
        ToolAction::FileRead {
            path: "notes.txt".into(),
            lines: Some(LineRange::new(1, 1).unwrap()),
        },
        ToolState::Pending,
    )
}

pub(super) fn usage_item(metric: UsageMetric, amount: UsageAmount) -> Item {
    item(ItemBody::Session {
        body: SessionBody::Usage {
            reading: UsageReading {
                metric,
                scope: UsageScope::Turn,
                amount,
            },
        },
    })
}

pub(super) fn money_item(amount: &str, currency: &str) -> Item {
    usage_item(
        UsageMetric::Cost,
        UsageAmount::Money {
            amount: amount.parse().unwrap(),
            currency: currency.parse().unwrap(),
        },
    )
}

pub(super) fn fixture_time() -> FirstSeenAtMs {
    FirstSeenAtMs::try_from(1_700_000_000_000).unwrap()
}

// The existing exact-byte cases also supply Rust values to the contract
// writer. Every original case remains runnable by its own test name.
macro_rules! item_scenarios {
    ($(fn $case:ident($examples:ident) $body:block)*) => {
        pub(super) fn collect_examples(corpus: &mut $crate::contract_item_fixtures::ItemExamples) {
            $({
                let $examples = &mut *corpus;
                $body
            })*
        }

        $(
            #[test]
            fn $case() {
                let mut corpus = $crate::contract_item_fixtures::ItemExamples::default();
                let $examples = &mut corpus;
                $body
            }
        )*
    };
}
pub(super) use item_scenarios;
