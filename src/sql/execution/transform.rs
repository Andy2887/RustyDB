use crate::common::Result;
use crate::sql::planner::Direction;
use crate::sql::planner::Expression;
use crate::storage::tuple::{Row, Rows};
use crate::types::field::Field;
use itertools::{izip, Itertools as _};

/// Filters the input rows (i.e. WHERE).
///
/// (Hint: look at the `iterator.rs` standard library API. There's a
/// method that returns an iterator that only emits elements that
/// satisfy a given predicate.)
pub fn filter(source: Rows, predicate: Expression) -> Rows {
    
    // Create a new iterator that filters rows based on the predicate
    let filtered_iter = source.filter_map(move |x| {
        // Handle the Result from the iterator
        let processed = x.and_then(|(rid, row)| {
            // Evaluate the predicate expression on this row
            let evaluation_result = predicate.evaluate(Some(&row))?;
            
            // Check what the predicate returned
            match evaluation_result {
                // If true, keep this row
                Field::Boolean(true) => Ok(Some((rid, row))),
                
                // If false or null, filter out this row
                Field::Boolean(false) | Field::Null => Ok(None),
                
                // If not a boolean, that's an error
                value => Err(crate::common::Error::InvalidInput(
                    format!("filter returned {value}, expected boolean")
                )),
            }
        });
        
        // Convert Result<Option<T>> to Option<Result<T>> for filter_map
        processed.transpose()
    });
    
    // Wrap in Box to return Rows type
    Box::new(filtered_iter)

}

/// Limits the result to the given number of rows (i.e. LIMIT).
///
/// (Hint: look at the `iterator.rs` standard library API. There's a
/// method that limits the iterator to a specified number of elements.)
pub fn limit(source: Rows, limit: usize) -> Rows {
    Box::new(source.take(limit))
}

/// Skips the given number of rows (i.e. OFFSET).
#[allow(dead_code)]
pub fn offset(source: Rows, offset: usize) -> Rows {
    Box::new(source.skip(offset))
}

/// Sorts the rows (i.e. ORDER BY).
pub fn order(source: Rows, order: Vec<(Expression, Direction)>) -> Result<Rows> {
    // We can't use sort_by_cached_key(), since expression evaluation is
    // fallible, and since we may have to vary the sort direction of each
    // expression. Precompute the sort values instead, and map them based on
    // the row index.
    let mut irows: Vec<_> = source
        .enumerate()
        .map(|(i, r)| r.map(|row| (i, row)))
        .try_collect()?;
    let mut sort_values = Vec::with_capacity(irows.len());
    for (_, (_rid, row)) in &irows {
        let values: Vec<_> = order
            .iter()
            .map(|(e, _)| e.evaluate(Some(&row)))
            .try_collect()?;
        sort_values.push(values)
    }

    irows.sort_by(|&(a, _), &(b, _)| {
        let dirs = order.iter().map(|(_, dir)| dir);
        for (a, b, dir) in izip!(&sort_values[a], &sort_values[b], dirs) {
            match a.cmp(b) {
                std::cmp::Ordering::Equal => {}
                order if *dir == Direction::Descending => return order.reverse(),
                order => return order,
            }
        }
        std::cmp::Ordering::Equal
    });

    Ok(Box::new(irows.into_iter().map(|(_, row)| Ok(row))))
}

/// Projects the rows using the given expressions (i.e. SELECT).
///
/// (Hint: The result of calling Expression::evaluate(row: Option<&Row>)
/// to evaluate the expression on a given row.)
/// (Hint 2: Each expression in expressions corresponds to a column that
/// the projection is selecting for. You'll want to build a projection
/// row from the results of calling each expression on a given row.)
pub fn project(source: Rows, expressions: Vec<Expression>) -> Rows {
    Box::new(source.map(move |item| {
        item.and_then(|(record_id, row)| {
            // Create a vector to hold the projected field values
            let mut projected_fields = Vec::with_capacity(expressions.len());
            
            // Evaluate each expression on the current row
            for expression in &expressions {
                let field = expression.evaluate(Some(&row))?;
                projected_fields.push(field);
            }
            
            // Create a new row from the projected fields
            Ok((record_id, Row::from(projected_fields)))
        })
    }))
}

/// Remaps source columns to target column indexes, or drops them if None.
pub fn remap(source: Rows, targets: Vec<Option<usize>>) -> Rows {
    let size = targets
        .iter()
        .filter_map(|v| *v)
        .map(|i| i + 1)
        .max()
        .unwrap_or(0);
    Box::new(source.map_ok(move |(rid, row)| {
        let mut out = vec![Field::Null; size];
        for (value, target) in row.into_iter().zip(&targets) {
            if let Some(index) = target {
                out[*index] = value;
            }
        }
        (rid, Row::from(out))
    }))
}
