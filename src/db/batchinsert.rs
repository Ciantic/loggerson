use std::{collections::HashMap, marker::PhantomData};

use rusqlite::{Connection, Params, Row, Statement};

struct BatchInsertQuery<'a, Key, P, PFun, Fun, Input, Output: 'a>
where
    Fun: FnOnce(&Row<'_>) -> rusqlite::Result<(Key, Output)> + Copy,
    PFun: (FnOnce(&Input) -> P) + Copy,
    P: Params,
{
    _t: PhantomData<Input>,
    _f: &'a PhantomData<i32>,
    _f2: PhantomData<PFun>,
    insert_sql: String,
    cache_select_sql: String,
    binder: Box<Fun>,
    params_binder: Box<PFun>,
    cache: HashMap<Input, Key>,
}

impl<'a, Key, P, PFun, Fun, Input, Output: 'a>
    BatchInsertQuery<'a, Key, P, PFun, Fun, Input, Output>
where
    Fun: FnOnce(&Row<'_>) -> rusqlite::Result<(Key, Output)> + Copy,
    PFun: (FnOnce(&Input) -> P) + Copy,
    P: Params,
{
    pub fn new(insert_sql: &str, cache_select_sql: &str, params_binder: PFun, binder: Fun) -> Self {
        BatchInsertQuery {
            _t: PhantomData,
            _f: &PhantomData,
            _f2: PhantomData,
            insert_sql: insert_sql.to_owned(),
            cache_select_sql: cache_select_sql.to_owned(),
            binder: Box::new(binder),
            params_binder: Box::new(params_binder),
            cache: HashMap::new(),
        }
    }

    pub fn insert(self, mut con: Connection, entries: &Vec<Input>) -> Vec<(Key, Output)> {
        let tx = con.transaction().unwrap();
        let mut insertor = tx.prepare_cached(&self.insert_sql).unwrap();
        // let cacher = tx.prepare_cached(&self.cache_select_sql).unwrap();

        entries
            .iter()
            .map(|i| {
                let o = insertor
                    .query_row(self.params_binder.as_ref()(i), |row| {
                        self.binder.as_ref()(row)
                    })
                    .unwrap();

                o
                // let (key, value) = self.binder.as_ref()(todo!(), i);
                // value
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::{params, Connection};

    use crate::db::batchinsert::BatchInsertQuery;

    #[test]
    fn it_works() {
        let c = Connection::open_in_memory().unwrap();
        c.execute_batch(
            "
        CREATE TABLE people (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT
        );
        ",
        )
        .unwrap();

        let biq = BatchInsertQuery::new(
            "INSERT INTO people (name) VALUES (?) RETURNING id",
            "Doo",
            |input: &String| [input.to_owned()], // TODO: This to_owned is not wanted!
            // |input| params![input],
            |row| Ok((row.get_unwrap::<_, i32>(0), ())),
        );

        biq.insert(
            c,
            &vec![
                "John".to_owned(),
                "Mary".to_owned(),
                "Test".to_owned(),
                "Doe".to_owned(),
            ],
        );
        // biq.insert(&vec![1, 2, 3]);
        assert_eq!(2 + 2, 4);
    }
}
