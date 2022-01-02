use std::{collections::HashMap, marker::PhantomData};

use rusqlite::{Connection, Params, Row, Rows, Statement, ToSql};

struct BatchQuery<Input, Value, FnBindParams, FnMapRow, const N: usize>
where
    FnMapRow: FnOnce(Rows) -> rusqlite::Result<Value> + Copy,
    for<'i> FnBindParams: (FnOnce(&'i Input) -> [&'i dyn ToSql; N]) + Copy,
{
    _i: PhantomData<Input>,
    sql: String,
    row_to_key: Box<FnMapRow>,
    bind_params: Box<FnBindParams>,
}

impl<Input, Value, FnBindParams, FnMapRow, const N: usize>
    BatchQuery<Input, Value, FnBindParams, FnMapRow, N>
where
    FnMapRow: FnOnce(Rows) -> rusqlite::Result<Value> + Copy,
    for<'i> FnBindParams: (FnOnce(&'i Input) -> [&'i dyn ToSql; N]) + Copy,
{
    pub fn new(insert_sql: &str, bind_params: FnBindParams, row_to_key: FnMapRow) -> Self {
        BatchQuery {
            _i: PhantomData,
            sql: insert_sql.to_owned(),
            row_to_key: Box::new(row_to_key),
            bind_params: Box::new(bind_params),
        }
    }

    pub fn query<'i>(
        &mut self,
        con: &Connection,
        entries: &Vec<&'i Input>,
    ) -> rusqlite::Result<Vec<(&'i Input, Value)>> {
        let mut stmt = con.prepare_cached(&self.sql)?;
        let mut ret: Vec<(&Input, Value)> = Vec::new();

        for input in entries.iter().copied() {
            let p = self.bind_params.as_ref()(input);
            for (index, v) in p.into_iter().enumerate() {
                stmt.raw_bind_parameter(index + 1, v)?;
            }
            let rows = stmt.raw_query();
            let key = self.row_to_key.as_ref()(rows)?;
            ret.push((input, key));
        }

        // let results = {
        //     entries
        //         .iter()
        //         .copied()
        //         .map(|input| {
        //             // I can't use query_row, query_map or query directly, because of this:
        //             // https://github.com/rusqlite/rusqlite/issues/1068
        //             //
        //             // Instead I call raw bind param manually
        //             let p = self.bind_params.as_ref()(input);
        //             for (index, v) in p.into_iter().enumerate() {
        //                 stmt.raw_bind_parameter(index + 1, v).unwrap();
        //             }
        //             let rows = stmt.raw_query();
        //             let key = self.row_to_key.as_ref()(rows)?;
        //             (input, key)
        //         })
        //         .collect()
        // };
        // results
        Ok(ret)
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

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

        // let mut tx = &mut c.transaction().unwrap();
        BatchQuery::new(
            "INSERT INTO people (name, address) VALUES (?, ?) RETURNING id",
            |input: &Person| [&input.name, &input.address],
            |mut rows| {
                let row = rows.next()?.unwrap();
                Ok(row.get::<_, i32>(0)?)
            },
        )
        .query(
            &c,
            &vec![
                &Person {
                    name: "John".to_owned(),
                    address: "Kukkaiskuja 123".to_owned(),
                },
                &Person {
                    name: "Mary".to_owned(),
                    address: "Homestreet 123".to_owned(),
                },
                &Person {
                    name: "Mary".to_owned(),
                    address: "Homestreet 123".to_owned(),
                },
            ],
        );

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
