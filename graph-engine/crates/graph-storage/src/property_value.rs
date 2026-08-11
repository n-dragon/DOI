//! Arrow cell → [`crate::PropertyValue`] deserialization (task ST3), one
//! function per [`ScalarType`] variant (spec §3.2). `iceberg`'s `to_arrow`
//! scan API (spec's chosen crate, task ST1) yields Parquet rows as Arrow
//! `RecordBatch`es, so this is the actual "Parquet row → PropertyValue"
//! boundary — just mediated by Arrow's columnar in-memory format rather
//! than raw Parquet.

use crate::{PropertyValue, StorageError};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float64Array, Int64Array, ListArray, StringArray,
    TimestampMicrosecondArray,
};
use graph_schema::ScalarType;

/// Reads the value of `array` at `row` as a [`PropertyValue`], per the
/// declared `ty`. `Vector` (§3.2, reserved for future embeddings and
/// explicitly unindexed in v1 — §5.2/§13) is read the same way as
/// `List<Float64>`: there's no dedicated `PropertyValue` variant for it
/// since nothing in v1 needs to distinguish the two once read.
pub(crate) fn read_property_value(
    array: &ArrayRef,
    row: usize,
    ty: &ScalarType,
) -> Result<PropertyValue, StorageError> {
    if array.is_null(row) {
        return Ok(PropertyValue::Null);
    }

    match ty {
        ScalarType::Int64 => downcast::<Int64Array>(array, ty)
            .map(|a| PropertyValue::Int64(a.value(row))),
        ScalarType::Float64 => downcast::<Float64Array>(array, ty)
            .map(|a| PropertyValue::Float64(a.value(row))),
        ScalarType::Bool => downcast::<BooleanArray>(array, ty)
            .map(|a| PropertyValue::Bool(a.value(row))),
        ScalarType::String => downcast::<StringArray>(array, ty)
            .map(|a| PropertyValue::String(a.value(row).to_string())),
        ScalarType::Timestamp => downcast::<TimestampMicrosecondArray>(array, ty)
            .map(|a| PropertyValue::Timestamp(a.value(row))),
        ScalarType::Bytes => downcast::<BinaryArray>(array, ty)
            .map(|a| PropertyValue::Bytes(a.value(row).to_vec())),
        ScalarType::List(elem_ty) => {
            let list = downcast::<ListArray>(array, ty)?;
            let elements = list.value(row);
            (0..elements.len())
                .map(|i| read_property_value(&elements, i, elem_ty))
                .collect::<Result<Vec<_>, _>>()
                .map(PropertyValue::List)
        }
        ScalarType::Vector { .. } => {
            let list = downcast::<ListArray>(array, ty)?;
            let elements = list.value(row);
            (0..elements.len())
                .map(|i| read_property_value(&elements, i, &ScalarType::Float64))
                .collect::<Result<Vec<_>, _>>()
                .map(PropertyValue::List)
        }
    }
}

fn downcast<'a, T: 'static>(
    array: &'a ArrayRef,
    ty: &ScalarType,
) -> Result<&'a T, StorageError> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        StorageError::NonConformingRow(format!(
            "column declared as {ty:?} does not have the matching Arrow array type"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn reads_each_scalar_variant() {
        let int_array: ArrayRef = Arc::new(Int64Array::from(vec![Some(42), None]));
        assert!(matches!(
            read_property_value(&int_array, 0, &ScalarType::Int64).unwrap(),
            PropertyValue::Int64(42)
        ));
        assert!(matches!(
            read_property_value(&int_array, 1, &ScalarType::Int64).unwrap(),
            PropertyValue::Null
        ));

        let float_array: ArrayRef = Arc::new(Float64Array::from(vec![1.5]));
        assert!(matches!(
            read_property_value(&float_array, 0, &ScalarType::Float64).unwrap(),
            PropertyValue::Float64(v) if v == 1.5
        ));

        let bool_array: ArrayRef = Arc::new(BooleanArray::from(vec![true]));
        assert!(matches!(
            read_property_value(&bool_array, 0, &ScalarType::Bool).unwrap(),
            PropertyValue::Bool(true)
        ));

        let string_array: ArrayRef = Arc::new(StringArray::from(vec!["Alice"]));
        match read_property_value(&string_array, 0, &ScalarType::String).unwrap() {
            PropertyValue::String(s) => assert_eq!(s, "Alice"),
            other => panic!("expected String, got {other:?}"),
        }

        let ts_array: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![1_700_000_000]));
        assert!(matches!(
            read_property_value(&ts_array, 0, &ScalarType::Timestamp).unwrap(),
            PropertyValue::Timestamp(1_700_000_000)
        ));

        let bytes_array: ArrayRef = Arc::new(BinaryArray::from(vec![b"abc".as_slice()]));
        match read_property_value(&bytes_array, 0, &ScalarType::Bytes).unwrap() {
            PropertyValue::Bytes(b) => assert_eq!(b, b"abc"),
            other => panic!("expected Bytes, got {other:?}"),
        }
    }

    #[test]
    fn reads_a_list_of_strings() {
        let list_array: ArrayRef = Arc::new(ListArray::from_iter_primitive::<
            arrow_array::types::Int64Type,
            _,
            _,
        >(vec![Some(vec![Some(1), Some(2), Some(3)])]));

        match read_property_value(&list_array, 0, &ScalarType::List(Box::new(ScalarType::Int64)))
            .unwrap()
        {
            PropertyValue::List(items) => {
                assert_eq!(items.len(), 3);
                assert!(matches!(items[0], PropertyValue::Int64(1)));
            }
            other => panic!("expected List, got {other:?}"),
        }
    }

    #[test]
    fn a_type_mismatch_is_a_non_conforming_row() {
        let int_array: ArrayRef = Arc::new(Int64Array::from(vec![42]));
        let err = read_property_value(&int_array, 0, &ScalarType::String).unwrap_err();
        assert!(matches!(err, StorageError::NonConformingRow(_)));
    }
}
