use std::{collections::HashMap, marker::PhantomData};

use rusqlite::{Connection, Statement};

struct BatchInsertQuery<'a, Key, Fun, Input, Output: 'a>
where
    Fun: FnOnce(&'a mut Statement, &Input) -> (Key, Output) + Copy,
{
    _t: PhantomData<Input>,
    _f: &'a PhantomData<i32>,
    insert_sql: String,
    cache_select_sql: String,
    binder: Box<Fun>,
    cache: HashMap<Input, Key>,
}

impl<'a, Key, Fun, Input, Output: 'a> BatchInsertQuery<'a, Key, Fun, Input, Output>
where
    Fun: FnOnce(&'a mut Statement, &Input) -> (Key, Output) + Copy,
{
    pub fn new(insert_sql: &str, cache_select_sql: &str, binder: Fun) -> Self {
        BatchInsertQuery {
            _t: PhantomData,
            _f: &PhantomData,
            insert_sql: insert_sql.to_owned(),
            cache_select_sql: cache_select_sql.to_owned(),
            binder: Box::new(binder),
            cache: HashMap::new(),
        }
    }

    pub fn insert(self, mut con: Connection, entries: &Vec<Input>) -> Vec<Output> {
        let tx = con.transaction().unwrap();
        let insertor = tx.prepare_cached(&self.insert_sql);
        let cacher = tx.prepare_cached(&self.cache_select_sql);

        entries
            .iter()
            .map(|i| {
                let (key, value) = self.binder.as_ref()(todo!(), i);
                value
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::params;

    use crate::db::batchinsert::BatchInsertQuery;

    #[test]
    fn it_works() {
        let biq = BatchInsertQuery::new("Foo", "Doo", |stmt, row: &String| {
            (3, "foo".to_owned())
            // todo!()
            // stmt.query_map(params![row], |r| Ok(r.get_unwrap::<_, i32>(0)))
        });
        // biq.insert(&vec![1, 2, 3]);
        assert_eq!(2 + 2, 4);
    }
}
