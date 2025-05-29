use crate::common::Result;
use crate::sql::planner::Expression;

use crate::storage::page::{RecordId, INVALID_RID};
use crate::storage::tuple::{Row, Rows};
use crate::types::field::Field;
use itertools::Itertools as _;
use std::collections::HashMap;
use std::iter::Peekable;

/// A nested loop join. Iterates over the right source for every row in the left
/// source, optionally filtering on the join predicate. If outer is true, and
/// there are no matches in the right source for a row in the left source, a
/// joined row with NULL values for the right source is returned (typically used
/// for a LEFT JOIN).
pub fn nested_loop(
    left: Rows,
    right: Rows,
    right_size: usize,
    predicate: Option<Expression>,
    outer: bool,
) -> Result<Rows> {
    Ok(Box::new(NestedLoopIterator::new(
        left, right, right_size, predicate, outer,
    )?))
}

/// NestedLoopIterator implements nested loop joins.
///
/// This could be trivially implemented with cartesian_product(), but we need
/// to handle the left outer join case where there is no match in the right
/// source.
#[derive(Clone)]
struct NestedLoopIterator {
    /// The left source.
    left: Peekable<Rows>,
    /// The right source.
    right: Rows,
    /// The initial right iterator state. Cloned to reset right.
    right_init: Rows,
    /// The column width of the right source.
    right_size: usize,
    /// True if a right match has been seen for the current left row.
    right_match: bool,
    /// The join predicate.
    predicate: Option<Expression>,
    /// If true, emit a row when there is no match in the right source.
    outer: bool,
}

impl NestedLoopIterator {
    fn new(
        left: Rows,
        right: Rows,
        right_size: usize,
        predicate: Option<Expression>,
        outer: bool,
    ) -> Result<Self> {
        let left = left.peekable();
        let right_init = right.clone();
        Ok(Self {
            left,
            right,
            right_init,
            right_size,
            right_match: false,
            predicate,
            outer,
        })
    }

    /// Returns the next joined row, if any.
    ///
    /// While there is a valid left row, look for a right-hand match to return.
    /// If there was no match for that row but this is an outer join, emit a row
    /// with right NULLs.
    fn try_next(&mut self) -> Result<Option<(RecordId, Row)>> {
        loop {
            // If we have a current left row, try to find a match
            if let Some(row_result) = self.left.peek(){
                let left_row = match row_result{
                    Ok((_record_id, row)) => row,
                    Err(_) => return Ok(None),
                };
                // Look for matches in the right iterator
                while let Some(right_result) = self.right.next(){
                    let (_, right_row) = right_result?;
                    
                    // Create combined row
                    let combined_row = Row::from(
                    left_row.iter()
                        .chain(right_row.iter())
                        .collect::<Vec<&Field>>()
                    );

                    // Check if predicate matches (if any)
                    let matches = if let Some(ref predicate) = self.predicate {
                        match predicate.evaluate(Some(&combined_row))? {
                            Field::Boolean(true) => true,
                            _ => false,
                        }
                    } else {
                        true // No predicate means always match
                    };
                    
                    if matches {
                        self.right_match = true;
                        return Ok(Some((INVALID_RID, combined_row)));
                    }
                }
                // No more right rows for this left row
                // Move to next left row and reset right iterator
                let left_result = self.left.next().transpose()?;
                if let Some((left_rid, left_row)) = left_result {
                    // If this was an outer join and no matches found, emit NULL row
                    if self.outer && !self.right_match {
                        let null_fields: Vec<Field> = (0..self.right_size)
                            .map(|_| Field::Null)
                            .collect();
                        let combined_row = Row::from(
                            left_row.iter()
                                .chain(null_fields.iter())
                                .cloned()
                                .collect::<Vec<Field>>()
                        );
                        self.right_match = false;
                        self.right = self.right_init.clone();
                        return Ok(Some((INVALID_RID, combined_row)));
                    }
                    
                    // Reset for next left row
                    self.right_match = false;
                    self.right = self.right_init.clone();
                } else {
                    return Ok(None); // No more left rows
                }

            } else{
                return Ok(None); // No more left rows
            }
        }

    }
}

impl Iterator for NestedLoopIterator {
    type Item = Result<(RecordId, Row)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

/// Executes a hash join. This builds a hash table of rows from the right source
/// keyed on the join value, then iterates over the left source and looks up
/// matching rows in the hash table. If outer is true, and there is no match
/// in the right source for a row in the left source, a row with NULL values
/// for the right source is emitted instead.
pub fn hash(
    left: Rows,
    left_column: usize,
    right: Rows,
    right_column: usize,
    right_size: usize,
    outer: bool,
) -> Result<Rows> {
    // Build the hash table from the right source.
    let mut rows = right;
    let mut right: HashMap<Field, Vec<Row>> = HashMap::new();
    while let Some((_, row)) = rows.next().transpose()? {
        let value = row.get_field(right_column)?.clone();
        if value.is_undefined() {
            continue; // NULL and NAN equality is always false
        }
        right.entry(value).or_default().push(row);
    }

    // Set up an iterator for an empty right row in the outer case.
    let empty = std::iter::repeat(Field::Null).take(right_size);

    // Set up the join iterator.
    let join = left.flat_map(move |result| -> Rows {
        // Pass through errors.
        let Ok((_, row)) = result else {
            return Box::new(std::iter::once(result));
        };
        // Join the left row with any matching right rows.
        match right.get(&row.get_field(left_column).unwrap()) {
            Some(matches) => Box::new(
                std::iter::once(row)
                    .cartesian_product(matches.clone())
                    .map(|(l, r)| {
                        (
                            INVALID_RID,
                            Row::from(l.iter().chain(r.iter()).collect::<Vec<&Field>>()),
                        )
                    })
                    .map(Ok),
            ),
            None if outer => Box::new(std::iter::once(Ok((
                INVALID_RID,
                Row::from(row.into_iter().chain(empty.clone()).collect::<Vec<_>>()),
            )))),
            None => Box::new(std::iter::empty()),
        }
    });
    Ok(Box::new(join))
}
