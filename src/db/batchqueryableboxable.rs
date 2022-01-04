use rusqlite::{Connection, Rows, ToSql};
use std::{borrow::Borrow, marker::PhantomData};

use super::batchqueryable::BatchQueryable;

pub trait BatchQueryableBoxable<Input, Value>
where
    Input: Borrow<Input>,
{
    fn query(&self, con: &Connection, entries: Vec<Input>) -> rusqlite::Result<Vec<Value>>;

    fn query_with_transaction(
        &self,
        con: &mut Connection,
        entries: Vec<Input>,
    ) -> rusqlite::Result<Vec<Value>>;
}

impl<T, Input, Value> BatchQueryableBoxable<Input, Value> for T
where
    T: BatchQueryable<Input, Value>,
{
    fn query(&self, con: &Connection, entries: Vec<Input>) -> rusqlite::Result<Vec<Value>> {
        BatchQueryable::query(&self as &T, con, entries.as_slice())
    }

    fn query_with_transaction(
        &self,
        con: &mut Connection,
        entries: Vec<Input>,
    ) -> rusqlite::Result<Vec<Value>> {
        BatchQueryable::query_with_transaction(&self as &T, con, entries.as_slice())
    }
}
