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

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    use crate::{db::batchquery::BatchQuery, LogEntry, Useragent};

    use super::BatchQueryableBoxable;

    pub struct BatchInsertor5 {
        // requests: Box<dyn BatchQueryableBoxable<User, i32>>,
        // users: Box<dyn BatchQueryableBoxable<User, i32>>,
        useragents: Box<dyn BatchQueryableBoxable<Useragent, i32>>,
        entrys: Box<dyn BatchQueryableBoxable<LogEntry, i32>>,
    }

    fn operate(con: &Connection, entrys: Box<dyn BatchQueryableBoxable<Useragent, i32>>) {
        let entries = vec![Useragent {
            value: "Foo".to_owned(),
        }];
        let iter = entries.into_iter();
        let a = BatchInsertor5 {
            useragents: Box::new(BatchQuery::new(
                "
                INSERT INTO
                useragents (value)
                VALUES (?)
                ON CONFLICT UPDATE SET Id = Id 
                RETURNING 1
            ",
                |r: &Useragent| [&r.value],
                |mut rows| {
                    let row = rows.next()?.unwrap();
                    Ok(row.get::<_, i32>(0)?)
                },
            )),
            entrys: Box::new(BatchQuery::new(
                "
                INSERT INTO
                entrys(timestamp, request_id, user_id)
                VALUES(?, ?, ?)
                ON CONFLICT DO NOTHING
            ",
                |r: &LogEntry| [&r.timestamp, &1, &2],
                |mut rows| {
                    let row = rows.next()?.unwrap();
                    Ok(row.get::<_, i32>(0)?)
                },
            )),
        };
    }
}
