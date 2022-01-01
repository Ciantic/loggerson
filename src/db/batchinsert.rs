use std::{collections::HashMap, marker::PhantomData};

use rusqlite::{Connection, Row, ToSql};

// struct DumParams<'a, const N: usize>([&'a dyn ToSql; N]);
// impl<'a, const N: usize> Sealed for DumParams<'a, N> {}

struct BatchInsertQuery<Key, PFun, Fun, Input, Output, const N: usize>
where
    Fun: FnOnce(&Row<'_>) -> rusqlite::Result<(Key, Output)> + Copy,
    for<'i> PFun: (FnOnce(&'i Input) -> [&'i dyn ToSql; N]) + Copy,
{
    insert_sql: String,
    cache_select_sql: String,
    binder: Box<Fun>,
    params_binder: Box<PFun>,
    cache: HashMap<Input, Key>,
}

impl<'a, Key, PFun, Fun, Input, Output: 'a, const N: usize>
    BatchInsertQuery<Key, PFun, Fun, Input, Output, N>
where
    Fun: FnOnce(&Row<'_>) -> rusqlite::Result<(Key, Output)> + Copy,
    for<'i> PFun: (FnOnce(&'i Input) -> [&'i dyn ToSql; N]) + Copy,
{
    pub fn new(insert_sql: &str, cache_select_sql: &str, params_binder: PFun, binder: Fun) -> Self {
        BatchInsertQuery {
            insert_sql: insert_sql.to_owned(),
            cache_select_sql: cache_select_sql.to_owned(),
            binder: Box::new(binder),
            params_binder: Box::new(params_binder),
            cache: HashMap::new(),
        }
    }

    pub fn insert(self, con: &mut Connection, entries: &Vec<&Input>) -> Vec<(Key, Output)> {
        let tx = con.transaction().unwrap();
        let results = {
            let mut insertor = tx.prepare_cached(&self.insert_sql).unwrap();

            entries
                .iter()
                .map(|i| {
                    let p = self.params_binder.as_ref()(i);

                    // I can't use query_row, query_map or query directly, because of this:
                    // https://github.com/rusqlite/rusqlite/issues/1068
                    //
                    // Instead I call raw bind param manually

                    for (index, v) in p.into_iter().enumerate() {
                        insertor.raw_bind_parameter(index + 1, v).unwrap();
                    }
                    let mut rows = insertor.raw_query();
                    let row = rows.next().unwrap().unwrap();
                    let (key, output) = self.binder.as_ref()(row).unwrap();
                    // self.cache.insert(k, v);
                    (key, output)
                })
                .collect()
        };
        tx.commit().unwrap();
        results
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    use crate::db::batchinsert::BatchInsertQuery;

    #[test]
    fn it_works() {
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

        let biq = BatchInsertQuery::new(
            "INSERT INTO people (name, address) VALUES (?, ?) RETURNING id",
            "Doo",
            |input: &Person| [&input.name, &input.address],
            |row| Ok((row.get_unwrap::<_, i32>(0), ())),
        );

        biq.insert(
            &mut c,
            &vec![
                &Person {
                    name: "John".to_owned(),
                    address: "Kukkaiskuja 123".to_owned(),
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
            ]
        );
    }
}
