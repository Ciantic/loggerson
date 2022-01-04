use rusqlite::{Connection, Rows, ToSql};
use std::{borrow::Borrow, marker::PhantomData};

pub trait BatchQueryable<Input, Value> {
    fn query<O, I>(&self, con: &Connection, entries: I) -> rusqlite::Result<O>
    where
        I: IntoIterator,
        I::Item: Borrow<Input>,
        O: FromIterator<Value>;

    fn query_with_transaction<I>(
        &self,
        con: &mut Connection,
        entries: I,
    ) -> rusqlite::Result<Vec<Value>>
    where
        I: IntoIterator,
        I::Item: Borrow<Input>;
}
