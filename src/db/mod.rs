use std::collections::HashMap;

use r2d2::{Pool, PooledConnection};
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{params, CachedStatement, Connection, Transaction};

use crate::{LogEntry, Request, User, Useragent};

const SCHEMA: &str = include_str!("schema.sql");

fn create_schema(conn: &PooledConnection<SqliteConnectionManager>) -> Result<(), rusqlite::Error> {
    conn.execute_batch(SCHEMA)
}

// static DBPOOL: OnceCell<Pool<SqliteConnectionManager>> = OnceCell::new();

pub fn init(path: &str) -> Result<Pool<SqliteConnectionManager>, rusqlite::Error> {
    // let manager = SqliteConnectionManager::memory();
    let manager = SqliteConnectionManager::file(path);
    let pool = r2d2::Pool::new(manager).unwrap();
    let conn = pool.get().unwrap();
    create_schema(&conn)?;
    // DBPOOL.set(pool.clone()).unwrap();
    Ok(pool)
}

pub struct BatchCache {
    pub useragents_cache: HashMap<Useragent, i32>,
    pub users_cache: HashMap<User, i32>,
    pub requests_cache: HashMap<Request, i32>,
}

impl BatchCache {
    pub fn new() -> Self {
        BatchCache {
            useragents_cache: HashMap::new(),
            users_cache: HashMap::new(),
            requests_cache: HashMap::new(),
        }
    }

    fn populate_requests_cache(&mut self, con: &Connection, first: i64, last: i64) {
        // Get all requests
        let mut reqstmt = con
            .prepare(
                "
                    SELECT DISTINCT r.id, r.method, r.url, r.status_code
                    FROM entrys e, requests r
                    WHERE e.request_id = r.id
                    AND e.timestamp >= ? AND e.timestamp <= ?",
            )
            .unwrap();

        let requests = reqstmt
            .query_map(params![first, last], |row| {
                Ok((
                    row.get::<_, i32>(3).unwrap(),
                    Request {
                        method: row.get(1).unwrap(),
                        url: row.get(2).unwrap(),
                        status_code: row.get(3).unwrap(),
                    },
                ))
            })
            .unwrap();

        for row in requests {
            if let Ok((id, req)) = row {
                self.requests_cache.insert(req, id);
            }
        }
    }

    fn populate_users_cache(&mut self, con: &Connection, first: i64, last: i64) {
        // Get all users and their useragents
        let mut reqstmt = con
            .prepare(
                "
                SELECT 
                    DISTINCT 
                    u.id as user_id, 
                    u.useragent_id, 
                    u.hash as user_hash, 
                    ua.value as useragent_value
                FROM entrys e, users u, useragents ua 
                WHERE 
                    e.user_id = u.id AND u.useragent_id = ua.id 
                    AND e.timestamp >= ? AND e.timestamp <= ?",
            )
            .unwrap();

        let query = reqstmt
            .query_map(params![first, last], |row| {
                Ok((
                    row.get::<_, i32>(0).unwrap(),
                    row.get::<_, i32>(1).unwrap(),
                    User {
                        hash: row.get(2).unwrap(),
                        useragent: Useragent {
                            value: row.get(3).unwrap(),
                        },
                    },
                ))
            })
            .unwrap();

        for row in query {
            if let Ok((user_id, useragent_id, user)) = row {
                self.useragents_cache
                    .insert(user.useragent.clone(), useragent_id);
                self.users_cache.insert(user, user_id);
            }
        }
    }

    pub fn populate(&mut self, con: &Connection, entries: &Vec<LogEntry>) {
        let first = entries.first().unwrap().timestamp;
        let last = entries.last().unwrap().timestamp;
        self.populate_requests_cache(con, first, last);
        self.populate_users_cache(con, first, last);
    }
}

pub struct BatchInsertor<'conn, 'cache> {
    requests_stmt: CachedStatement<'conn>,
    useragents_stmt: CachedStatement<'conn>,
    users_stmt: CachedStatement<'conn>,
    entrys_stmt: CachedStatement<'conn>,
    cache: &'cache mut BatchCache,
    tx: &'conn Transaction<'conn>,
}

impl<'conn, 'cache> BatchInsertor<'conn, 'cache> {
    pub fn new(
        tx: &'conn Transaction,
        cache: &'cache mut BatchCache,
    ) -> BatchInsertor<'conn, 'cache> {
        let requests_stmt = tx
            .prepare_cached(
                "
                INSERT INTO 
                requests(method, url, status_code) 
                VALUES(?, ?, ?) RETURNING id
            ",
            )
            .unwrap();

        let useragents_stmt = tx
            .prepare_cached(
                "
                INSERT INTO 
                useragents(value) 
                VALUES(?) RETURNING id
            ",
            )
            .unwrap();

        let users_stmt = tx
            .prepare_cached(
                "
                INSERT INTO
                users(hash, useragent_id)
                VALUES(?, ?) RETURNING id
                ",
            )
            .unwrap();

        let entrys_stmt = tx
            .prepare_cached(
                "
                INSERT INTO
                entrys(timestamp, request_id, user_id)
                VALUES(?, ?, ?)
                ON CONFLICT DO NOTHING
            ",
            )
            .unwrap();

        BatchInsertor {
            entrys_stmt,
            requests_stmt,
            useragents_stmt,
            users_stmt,
            cache,
            tx,
        }
    }

    fn get_request_id(&mut self, request: &Request) -> i32 {
        if let Some(v) = self.cache.requests_cache.get(&request) {
            // println!("HIT CACHE request_id");
            return v.to_owned();
        }

        let id = self
            .requests_stmt
            .query_row(
                params![&request.method, &request.url, &request.status_code],
                |row| Ok(row.get_unwrap(0)),
            )
            .unwrap();
        self.cache.requests_cache.insert(request.clone(), id);
        id
    }

    fn get_useragent_id(&mut self, useragent: &Useragent) -> i32 {
        let key = useragent.clone();
        if let Some(v) = self.cache.useragents_cache.get(&key) {
            // println!("HIT CACHE useragent_id");
            return v.to_owned();
        }

        let id = self
            .useragents_stmt
            .query_row(params![&useragent.value], |row| Ok(row.get_unwrap(0)))
            .unwrap();

        self.cache.useragents_cache.insert(key, id);
        id
    }

    fn get_user_id(&mut self, user: &User) -> i32 {
        let key = user.clone();
        if let Some(v) = self.cache.users_cache.get(&key) {
            // println!("HIT CACHE user_id");
            return v.to_owned();
        }

        let useragent_id = self.get_useragent_id(&user.useragent);
        let id = self
            .users_stmt
            .query_row(params![&user.hash, &useragent_id], |row| {
                Ok(row.get_unwrap(0))
            })
            .unwrap();
        self.cache.users_cache.insert(key, id);
        id
    }

    fn add(&mut self, entry: &LogEntry) {
        let request_id = self.get_request_id(&entry.request);
        let user_id: i32 = self.get_user_id(&entry.user);
        self.entrys_stmt
            .execute(params![&entry.timestamp, &request_id, &user_id])
            .unwrap();
    }

    // pub fn add_entries<'a, I>(&mut self, entries: I)
    // where
    //     I: Iterator<Item = &'a LogEntry>,
    // {

    // }

    pub fn add_entries(&mut self, entries: &Vec<LogEntry>) {
        self.cache.populate(&self.tx, entries);
        {
            println!(
                "Cache? {} {} {}",
                &self.cache.requests_cache.len(),
                &self.cache.users_cache.len(),
                &self.cache.useragents_cache.len()
            );

            for (n, r) in entries.iter().enumerate() {
                if n % 1000 == 0 {
                    println!("Sqlite add calls {}", n);
                }
                self.add(&r);
            }
            println!("ADDED A CHUNK");
        }
    }
}
