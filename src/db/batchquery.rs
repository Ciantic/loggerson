use std::marker::PhantomData;

use rusqlite::{Connection, Rows, ToSql, Transaction};

pub trait BatchQueryable<'i, Input: 'i, Value> {
    fn query<O, I>(&self, con: &Connection, entries: I) -> rusqlite::Result<O>
    where
        I: IntoIterator<Item = &'i Input>,
        O: FromIterator<Value>;

    fn query_with_transaction<I>(
        &self,
        con: &mut Connection,
        entries: I,
    ) -> rusqlite::Result<Vec<Value>>
    where
        I: IntoIterator<Item = &'i Input>;
}

pub struct BatchQuery<Input, Value, FnBindParams, FnMapRow, const N: usize>
where
    FnMapRow: FnOnce(Rows) -> rusqlite::Result<Value> + Copy,
    for<'i> FnBindParams: (FnOnce(&'i Input) -> [&'i dyn ToSql; N]) + Copy,
{
    _i: PhantomData<Input>,
    sql: String,
    row_to_value: Box<FnMapRow>,
    bind_params: Box<FnBindParams>,
}

impl<'i, Input: 'i, Value, FnBindParams, FnMapRow, const N: usize>
    BatchQuery<Input, Value, FnBindParams, FnMapRow, N>
where
    FnMapRow: FnOnce(Rows) -> rusqlite::Result<Value> + Copy,
    for<'a> FnBindParams: (FnOnce(&'a Input) -> [&'a dyn ToSql; N]) + Copy,
{
    pub fn new(insert_sql: &str, bind_params: FnBindParams, row_to_value: FnMapRow) -> Self {
        BatchQuery {
            _i: PhantomData,
            sql: insert_sql.to_owned(),
            row_to_value: Box::new(row_to_value),
            bind_params: Box::new(bind_params),
        }
    }
}

impl<'i, Input: 'i, Value, FnBindParams, FnMapRow, const N: usize> BatchQueryable<'i, Input, Value>
    for BatchQuery<Input, Value, FnBindParams, FnMapRow, N>
where
    FnMapRow: FnOnce(Rows) -> rusqlite::Result<Value> + Copy,
    for<'a> FnBindParams: (FnOnce(&'a Input) -> [&'a dyn ToSql; N]) + Copy,
{
    fn query<O, I>(&self, con: &Connection, entries: I) -> rusqlite::Result<O>
    where
        I: IntoIterator<Item = &'i Input>,
        O: FromIterator<Value>,
    {
        let mut stmt = con.prepare_cached(&self.sql)?;
        let results = entries.into_iter().map(|input| -> rusqlite::Result<_> {
            let p = self.bind_params.as_ref()(input);
            for (index, v) in p.into_iter().enumerate() {
                stmt.raw_bind_parameter(index + 1, v).unwrap();
            }
            let rows = stmt.raw_query();
            let key = self.row_to_value.as_ref()(rows)?;
            Ok(key)
        });

        Result::from_iter(results)
    }

    fn query_with_transaction<I>(
        &self,
        con: &mut Connection,
        entries: I,
    ) -> rusqlite::Result<Vec<Value>>
    where
        I: IntoIterator<Item = &'i Input>,
    {
        // In order to commit the transaction, values need to be collected
        let tx = con.transaction()?;
        let v = self.query::<Vec<_>, _>(&tx, entries)?;
        tx.commit()?;
        Ok(v)
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Borrow;

    use rusqlite::Connection;

    use crate::db::batchquery::BatchQueryable;

    use super::BatchQuery;

    #[test]
    fn it_works() {
        #[derive(PartialEq, Eq, Hash, Clone)]
        struct Person {
            name: String,
            address: String,
        }

        let mut c = Connection::open_in_memory().unwrap();
        c.execute_batch(
            "
            CREATE TABLE people (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                address TEXT
            );
        ",
        )
        .unwrap();

        let ids = BatchQuery::new(
            "INSERT INTO people (name, address) VALUES (?, ?) RETURNING id",
            |input: &Person| [&input.name, &input.address],
            |mut rows| {
                let row = rows.next()?.unwrap();
                Ok(row.get::<_, i32>(0)?)
            },
        )
        .query_with_transaction(
            &mut c,
            vec![
                Person {
                    name: "John".to_owned(),
                    address: "Kukkaiskuja 123".to_owned(),
                },
                Person {
                    name: "Mary".to_owned(),
                    address: "Homestreet 123".to_owned(),
                },
                Person {
                    name: "Mary".to_owned(),
                    address: "Homestreet 123".to_owned(),
                },
            ]
            .as_slice(),
        )
        .unwrap();

        assert_eq!(ids.iter().collect::<Vec<_>>(), vec![&1, &2, &3]);

        let mut stmt = c.prepare(&"SELECT * FROM people").unwrap();
        let results = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, i32>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                ))
            })
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<_>>();
        assert_eq!(
            results,
            vec![
                (1, "John".to_owned(), "Kukkaiskuja 123".to_owned()),
                (2, "Mary".to_owned(), "Homestreet 123".to_owned()),
                (3, "Mary".to_owned(), "Homestreet 123".to_owned()),
            ]
        );
    }
}
