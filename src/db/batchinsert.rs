use std::{collections::HashMap, marker::PhantomData};

use rusqlite::{Connection, Params, Row, Statement, ToSql};

struct BatchInsertQuery<Input, Key, FnInsertInputToParams, FnInsertRowToKey, const N: usize>
where
    FnInsertRowToKey: FnOnce(Option<&Row<'_>>) -> rusqlite::Result<Key> + Copy,
    for<'i> FnInsertInputToParams: (FnOnce(&'i Input) -> [&'i dyn ToSql; N]) + Copy,
{
    insert_sql: String,
    cache_select_sql: String,
    insert_row_to_key: Box<FnInsertRowToKey>,
    insert_params: Box<FnInsertInputToParams>,
    cache: HashMap<Input, Key>,
}

impl<Input, Key, FnInsertInputToParams, FnInsertRowToKey, const N: usize>
    BatchInsertQuery<Input, Key, FnInsertInputToParams, FnInsertRowToKey, N>
where
    FnInsertRowToKey: FnOnce(Option<&Row<'_>>) -> rusqlite::Result<Key> + Copy,
    for<'i> FnInsertInputToParams: (FnOnce(&'i Input) -> [&'i dyn ToSql; N]) + Copy,
    Input: Eq + std::hash::Hash + Clone,
    Key: Clone, // Or is Copy better?
{
    pub fn new(
        insert_sql: &str,
        insert_params: FnInsertInputToParams,
        insert_row_to_key: FnInsertRowToKey,
        cache_select_sql: &str,
    ) -> Self {
        BatchInsertQuery {
            insert_sql: insert_sql.to_owned(),
            insert_row_to_key: Box::new(insert_row_to_key),
            insert_params: Box::new(insert_params),
            cache_select_sql: cache_select_sql.to_owned(),
            cache: HashMap::new(),
        }
    }

    pub fn insert(&mut self, con: &mut Connection, entries: &Vec<&Input>) -> Vec<Key> {
        // TODO: Proper Result return type

        // TODO: Run Cache SQL, get [Key, Input]... values, then store these to
        // `cache` before insert

        // TODO: What if only part of the input value is unique? Maybe I need
        // also a FnOnce(Input) -> CacheKey? (Key is Insertion key, not cache
        // key)

        let tx = con.transaction().unwrap();
        let results = {
            let mut insertor = tx.prepare_cached(&self.insert_sql).unwrap();

            entries
                .iter()
                .copied()
                .map(|i| {
                    if let Some(cached_value) = self.cache.get(i) {
                        return cached_value.clone();
                    }

                    // I can't use query_row, query_map or query directly, because of this:
                    // https://github.com/rusqlite/rusqlite/issues/1068
                    //
                    // Instead I call raw bind param manually
                    let p = self.insert_params.as_ref()(i);
                    for (index, v) in p.into_iter().enumerate() {
                        insertor.raw_bind_parameter(index + 1, v).unwrap();
                    }
                    let mut rows = insertor.raw_query();
                    let row = rows.next().unwrap();
                    let key = self.insert_row_to_key.as_ref()(row).unwrap();
                    self.cache.insert(i.to_owned().to_owned(), key.to_owned());
                    key
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

        let mut biq = BatchInsertQuery::new(
            "INSERT INTO people (name, address) VALUES (?, ?) RETURNING id",
            |input: &Person| [&input.name, &input.address],
            |row| Ok(row.unwrap().get::<_, i32>(0)?),
            "SELECT id, name, address FROM people WHERE name != ?",
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
                // This is not inserted, as it's already cached
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
