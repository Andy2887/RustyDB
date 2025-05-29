use std::collections::BTreeMap;

use crate::common::Result;
use crate::sql::engine::Transaction;
use crate::sql::planner::Expression;
use crate::storage::page::RecordId;
use crate::storage::tuple::Rows;
use crate::types::Table;

/// Deletes rows, taking primary keys from the source (i.e. DELETE) using the
/// primary_key column index. Returns the number of rows deleted.
pub fn delete(txn: &impl Transaction, table: String, source: Rows) -> Result<u64> {
    // Create a new vector to store record_ids and store number of records
    let mut record_ids = Vec::new();
    let mut record_num = 0;
    
    // Collect all record IDs
    for item in source{
        let (record_id, _row) = item?;
        record_ids.push(record_id);
        record_num += 1;
    }

    let _ = txn.delete(&table, &record_ids);

    Ok(record_num)
    
}

/// Inserts rows into a table (i.e. INSERT) from the given source.
/// Returns the record IDs corresponding to the rows inserted into the table.
pub fn insert(txn: &impl Transaction, table: Table, source: Rows) -> Result<Vec<RecordId>> {
    
    // Get table name and create a new vector to store rows
    let table_name = table.name();
    let mut vec_rows = Vec::new();

    // Collect all record IDs
    for item in source{
        let (_record_id, row) = item?;
        vec_rows.push(row);
    }

    txn.insert(table_name, vec_rows)
}

/// Updates rows passed in from the source (i.e. UPDATE). Returns the number of
/// rows updated.
///
/// Hint: `<T,E> Option<Result<T,E>>::transpose(self) -> Result<Option<T>, E>` and
/// the `?` operator might be useful here. An example of `transpose` from the docs:
/// ```
/// #[derive(Debug, Eq, PartialEq)]
/// struct SomeErr;
///
/// let x: Result<Option<i32>, SomeErr> = Ok(Some(5));
/// let y: Option<Result<i32, SomeErr>> = Some(Ok(5));
/// assert_eq!(x, y.transpose());
/// ```
pub fn update(
    txn: &impl Transaction,
    table: String,
    mut source: Rows,
    expressions: Vec<(usize, Expression)>,
) -> Result<u64> {
    
    let mut updates = BTreeMap::new();

    for item in source{
        let (record_id, mut row) = item?;
        for (column_index, expression) in &expressions{
            let new_value = expression.evaluate(Some(&row))?;
            row.update_field(*column_index, new_value)?;
        }
        updates.insert(record_id, row);
    }

    // Get the count before calling update
    let count = updates.len() as u64;
    
    // Apply all updates to the database
    txn.update(&table, updates)?;
    
    Ok(count)

}
