//! Shared property-vs-literal comparison, used everywhere a
//! [`PropertyFilter`]/[`ComparisonOp`] needs to be evaluated against an
//! already-hydrated [`PropertyValue`] rather than pushed through
//! `graph-index`'s `PropertyIndex` (§5.2) — `distributed_executor`'s
//! `WHERE` handling (task Q6) and `local_executor`'s anti-join check
//! (least-privilege-via-telemetry use case) both need exactly this,
//! against node properties and edge properties respectively.

use graph_dsl::{ComparisonOp, Literal, PropertyFilter};
use graph_storage::PropertyValue;
use std::collections::BTreeMap;

pub(crate) fn evaluate_filter(
    properties: &BTreeMap<String, PropertyValue>,
    filter: &PropertyFilter,
) -> bool {
    match properties.get(&filter.property) {
        Some(value) => literal_matches(value, filter.op, &filter.value),
        None => false,
    }
}

pub(crate) fn literal_matches(value: &PropertyValue, op: ComparisonOp, literal: &Literal) -> bool {
    use std::cmp::Ordering;
    let ordering = match (value, literal) {
        (PropertyValue::Int64(v), Literal::Int64(l)) => v.partial_cmp(l),
        (PropertyValue::Float64(v), Literal::Float64(l)) => v.partial_cmp(l),
        (PropertyValue::Bool(v), Literal::Bool(l)) => Some(v.cmp(l)),
        (PropertyValue::String(v), Literal::String(l)) => Some(v.as_str().cmp(l.as_str())),
        // *(least-privilege-via-telemetry use case, scoping decision)*
        // No `datetime()`/`duration()` expression language in v1's DSL
        // (documented in `schema/least_privilege.graphidl`) — a
        // `Timestamp` property compares against a plain string literal
        // holding an absolute RFC3339 timestamp instead
        // (`a.last_seen >= "2024-05-01T00:00:00Z"`), parsed here rather
        // than at grammar/parse time so `Literal::String` stays the one
        // string literal representation the rest of the DSL already
        // understands. An unparseable string never matches, same
        // "doesn't match rather than panics" posture as a genuine type
        // mismatch below.
        (PropertyValue::Timestamp(v), Literal::String(l)) => {
            parse_rfc3339_micros(l).map(|parsed| v.cmp(&parsed))
        }
        // Type mismatch between the stored property and the filter
        // literal — the DSL validator (D7-D9) already rejects this
        // combination before a plan is ever built, so this is
        // unreachable in practice; treated as "doesn't match" rather
        // than panicking, since a partition-side data/schema drift
        // shouldn't take a query down.
        _ => None,
    };
    let Some(ordering) = ordering else {
        return false;
    };
    match op {
        ComparisonOp::Eq => ordering == Ordering::Equal,
        ComparisonOp::Ne => ordering != Ordering::Equal,
        ComparisonOp::Lt => ordering == Ordering::Less,
        ComparisonOp::Lte => ordering != Ordering::Greater,
        ComparisonOp::Gt => ordering == Ordering::Greater,
        ComparisonOp::Gte => ordering != Ordering::Less,
    }
}

fn parse_rfc3339_micros(s: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.timestamp_micros())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_timestamp_property_matches_an_rfc3339_string_literal() {
        // 2024-05-01T00:00:00Z in epoch microseconds.
        let micros = chrono::DateTime::parse_from_rfc3339("2024-05-01T00:00:00Z")
            .unwrap()
            .timestamp_micros();
        assert!(literal_matches(
            &PropertyValue::Timestamp(micros + 1),
            ComparisonOp::Gte,
            &Literal::String("2024-05-01T00:00:00Z".to_string()),
        ));
        assert!(!literal_matches(
            &PropertyValue::Timestamp(micros - 1),
            ComparisonOp::Gte,
            &Literal::String("2024-05-01T00:00:00Z".to_string()),
        ));
    }

    #[test]
    fn an_unparseable_string_never_matches_a_timestamp_property() {
        assert!(!literal_matches(
            &PropertyValue::Timestamp(0),
            ComparisonOp::Eq,
            &Literal::String("not a date".to_string()),
        ));
    }
}
