use crate::common::Result;
use crate::sql::engine::{Catalog, Transaction};
use crate::sql::execution::{aggregate, join, source, transform};
use crate::sql::planner::{BoxedNode, Node, Plan};
use crate::storage::page::RecordId;
use crate::storage::tuple::Rows;
use crate::types::field::Label;

use super::write;

/// Executes a query plan.
///
/// Takes both a catalog and transaction as parameters, even though a transaction
/// implements the Catalog trait, to separate the concerns of `catalog` to planning
/// and `txn` to execution.
///
/// Hint: `execute(source, txn)?` returns a `Rows` source iterator, which you might
/// need for some of the plans. (The `execute` method actually returns `Result<Rows>`,
/// but the `?` operator will automatically unwrap the result if it's an `Ok(Rows)`
/// value. Otherwise, the method will immediately exit and return the `Err()` value
/// returned from `execute`.) For more information about the try-operator `?`, see:
/// - https://doc.rust-lang.org/rust-by-example/std/result/question_mark.html
/// - https://stackoverflow.com/questions/42917566/what-is-this-question-mark-operator-about
pub fn execute_plan(
    plan: Plan,
    catalog: &impl Catalog,
    txn: &impl Transaction,
) -> Result<ExecutionResult> {
    Ok(match plan {
        // Creates a table with the given schema, returning a `CreateTable` execution
        // result if the table creation is successful.
        //
        // You'll need to handle the case when `Catalog::create_table` returns an Error
        // (hint: use the ? operator).
        Plan::CreateTable { schema } => {
            let name = schema.name().to_string();
            catalog.create_table(schema)?;
            ExecutionResult::CreateTable { name }
        }
        // Deletes the rows emitted from the source node from the given table.
        //
        // Hint: you'll need to use the `write::delete` method that you also have implement,
        // which returns the number of rows that were deleted if successful (another hint:
        // use the ? operator. Last reminder!).
        Plan::Delete { table, source } => {
            let result_rows = execute(source, txn)?;
            let count = write::delete(txn, table, result_rows)?;
            ExecutionResult::Delete { count }
        }
        // Drops the given table.
        //
        // Returns an error if the table does not exist unless `if_exists` is true.
        Plan::DropTable { table, if_exists } => {
            let existed = catalog.drop_table(&table, if_exists)?;
            
            if !existed && !if_exists{
                return Err(crate::common::Error::InvalidInput(format!("Table does not exists!")));
            }

            ExecutionResult::DropTable {
                name: table,
                existed,
            }
        }
        // Inserts the rows emitted from the source node into the given table.
        //
        // Hint: you'll need to use the `write::insert` method that you have to implement,
        // which returns the record id's corresponding to the rows that were inserted into
        // the table.
        Plan::Insert { table, source } => {
            let result_rows = execute(source, txn)?;
            let record_ids = write::insert(txn, table, result_rows)?;
            let count = record_ids.len() as u64;
            ExecutionResult::Insert { count, record_ids }
        }
        // Obtains a `Rows` iterator of the emitted rows and the emitted rows' corresponding
        // column labels from the root node, packaging the two as an `ExecutionResult::Select`.
        //
        // Hint: the i'th column label of a row emitted from the root can be obtained by calling
        // `root.column_label(i)`.
        Plan::Select(root) => {
            let column_count = root.columns();
            let mut column_labels = Vec::new();
            for i in 0..column_count{
                column_labels.push(root.column_label(i));
            }
            let result_rows = execute(root, txn)?;
            ExecutionResult::Select { rows: result_rows, columns: column_labels }
        }
        // Updates the rows emitted from the source node in the given table.
        //
        // Hint: you'll have to use the `write::update` method that you have implement, which
        // returns the number of rows update if successful.
        Plan::Update {
            table,
            source,
            expressions,
        } => {
            let result_rows = execute(source, txn)?;
            let count = write::update(txn, table.name().to_string(), result_rows, expressions)?;
            ExecutionResult::Update { count }
        }
    })
}

/// Recursively executes a query plan node, returning a tuple iterator.
///
/// Tuples stream through the plan node tree from the branches to the root. Nodes
/// recursively pull input rows upwards from their child node(s), process them,
/// and hand the resulting rows off to their parent node.
pub fn execute(node: BoxedNode, txn: &impl Transaction) -> Result<Rows> {
    Ok(match *node.inner {
        Node::Aggregate {
            source,
            group_by,
            aggregates,
        } => {
            let source = execute(source, txn)?;
            aggregate::aggregate(source, group_by, aggregates)?
        }

        Node::Filter { source, predicate } => {
            let result_rows = execute(source, txn)?;
            transform::filter(result_rows, predicate)
        }

        Node::HashJoin {
            left,
            left_column,
            right,
            right_column,
            outer,
        } => {
            let right_size = right.columns();
            let left = execute(left, txn)?;
            let right = execute(right, txn)?;
            join::hash(left, left_column, right, right_column, right_size, outer)?
        }

        Node::IndexLookup {
            table: _table,
            column: _column,
            values: _values,
            alias: _,
        } => {
            todo!();
        }

        Node::KeyLookup {
            table: _table,
            keys: _keys,
            alias: _,
        } => {
            todo!();
        }

        Node::Limit { source, limit } => {
            let result_rows = execute(source, txn)?;
            transform::limit(result_rows, limit)
        }

        Node::NestedLoopJoin {
            left,
            right,
            predicate,
            outer,
        } => {
            let right_size = right.columns();
            let left = execute(left, txn)?;
            let right = execute(right, txn)?;
            join::nested_loop(left, right, right_size, predicate, outer)?
        }

        Node::Nothing { .. } => source::nothing(),

        Node::Offset {
            source: _source,
            offset: _offset,
        } => {
            todo!();
        }

        Node::Order {
            source,
            key: orders,
        } => {
            let source = execute(source, txn)?;
            transform::order(source, orders)?
        }

        Node::Projection {
            source,
            expressions,
            aliases: _,
        } => {
            let source_rows = execute(source, txn)?;
            transform::project(source_rows, expressions)
        }

        Node::Remap { source, targets } => {
            let source = execute(source, txn)?;
            transform::remap(source, targets)
        }

        Node::Scan {
            table,
            filter,
            alias: _,
        } => {
            let result_rows = txn.scan(table.name(), filter)?;
            result_rows
        }

        Node::Values { rows } => source::values(rows),
    })
}

/// A plan execution result.
pub enum ExecutionResult {
    CreateTable {
        name: String,
    },
    DropTable {
        name: String,
        existed: bool,
    },
    Delete {
        count: u64,
    },
    Insert {
        count: u64,
        record_ids: Vec<RecordId>,
    },
    Update {
        count: u64,
    },
    Select {
        rows: Rows,
        columns: Vec<Label>,
    },
}
