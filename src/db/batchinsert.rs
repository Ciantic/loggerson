use std::marker::PhantomData;

use rusqlite::Statement;

struct BatchInsertQuery<T> {
    _t: PhantomData<T>,
}

impl<T> BatchInsertQuery<T> {
    pub fn new<'a, F, R: 'a>(insert_sql: &str, cache_select_sql: &str, binder: F) -> Self
    where
        F: Fn(&'a mut Statement, T) -> R,
    {
        BatchInsertQuery { _t: PhantomData }
    }

    pub fn insert(entries: Vec<T>) {}
}

#[cfg(test)]
mod tests {
    use rusqlite::params;

    use crate::db::batchinsert::BatchInsertQuery;

    #[test]
    fn it_works() {
        BatchInsertQuery::<i32>::new("Foo", "Doo", |stmt, row| {
            stmt.query_map(params![row], |r| Ok(r.get_unwrap::<_, i32>(0)))
        });
        assert_eq!(2 + 2, 4);
    }
}
