use std::{collections::HashMap, net::IpAddr};

use itertools::Itertools;
use r2d2::{Pool, PooledConnection};
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{params, Batch, CachedStatement, Transaction};
use sha2::{Digest, Sha256};

use crate::LogEntry;

const SCHEMA: &str = include_str!("schema.sql");

fn create_schema(conn: &PooledConnection<SqliteConnectionManager>) -> Result<(), rusqlite::Error> {
    conn.execute_batch(SCHEMA)
}

pub fn init(path: &str) -> Result<Pool<SqliteConnectionManager>, rusqlite::Error> {
    // let manager = SqliteConnectionManager::memory();
    let manager = SqliteConnectionManager::file(path);
    let pool = r2d2::Pool::new(manager).unwrap();
    let conn = pool.get().unwrap();
    create_schema(&conn)?;
    Ok(pool)
}

pub struct BatchCache {
    pub useragents_cache: HashMap<String, i32>,
    pub users_cache: HashMap<String, i32>,
    pub requests_cache: HashMap<(String, String, i32), i32>,
}

impl BatchCache {
    pub fn new() -> Self {
        BatchCache {
            useragents_cache: HashMap::new(),
            users_cache: HashMap::new(),
            requests_cache: HashMap::new(),
        }
    }

    pub fn populate(&mut self, con: PooledConnection<SqliteConnectionManager>) {
        // TODO: Populate caches...
    }
}

pub struct BatchInsertor<'conn, 'cache> {
    add_requests: CachedStatement<'conn>,
    add_useragents: CachedStatement<'conn>,
    add_users: CachedStatement<'conn>,
    add_entrys: CachedStatement<'conn>,
    cache: &'cache mut BatchCache,
}

impl<'conn, 'cache> BatchInsertor<'conn, 'cache> {
    pub fn new(
        conn: &'conn Transaction,
        cache: &'cache mut BatchCache,
    ) -> BatchInsertor<'conn, 'cache> {
        // It is expensive to check from DB all the time if the stuff exists,
        // thus the ID's are cached

        let add_requests = conn
            .prepare_cached(
                "
                INSERT INTO 
                requests(method, url, status_code) 
                VALUES(?, ?, ?) 
                ON CONFLICT DO UPDATE SET id=id RETURNING id
            ",
            )
            .unwrap();
        let add_useragents = conn
            .prepare_cached(
                "
                INSERT INTO 
                useragents(value) 
                VALUES(?) 
                ON CONFLICT DO UPDATE SET id=id RETURNING id
            ",
            )
            .unwrap();

        let add_users = conn
            .prepare_cached(
                "
                INSERT INTO
                users(hash, useragent_id)
                VALUES(?, ?)
                ON CONFLICT DO UPDATE SET id=id RETURNING id
                ",
            )
            .unwrap();

        let add_entrys = conn
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
            add_entrys,
            add_requests,
            add_useragents,
            add_users,
            cache,
        }
    }

    fn get_request_id(&mut self, entry: &LogEntry) -> i32 {
        let key = (
            entry.method.clone(),
            entry.url.clone(),
            entry.status.clone(),
        );
        if let Some(v) = self.cache.requests_cache.get(&key) {
            // println!("HIT CACHE request_id");
            return v.to_owned();
        }

        let id = self
            .add_requests
            .query_row(params![&entry.method, &entry.url, &entry.status], |row| {
                Ok(row.get_unwrap(0))
            })
            .unwrap();
        self.cache.requests_cache.insert(key, id);
        id
    }

    fn get_useragent_id(&mut self, entry: &LogEntry) -> i32 {
        let key = entry.useragent.clone();
        if let Some(v) = self.cache.useragents_cache.get(&key) {
            // println!("HIT CACHE useragent_id");
            return v.to_owned();
        }

        let id = self
            .add_useragents
            .query_row(params![&entry.useragent], |row| Ok(row.get_unwrap(0)))
            .unwrap();

        self.cache.useragents_cache.insert(key, id);
        id
    }

    fn get_user_id(&mut self, entry: &LogEntry) -> i32 {
        let key = entry.user_hash.clone();
        if let Some(v) = self.cache.users_cache.get(&key) {
            // println!("HIT CACHE user_id");
            return v.to_owned();
        }

        let useragent_id = self.get_useragent_id(&entry);
        let id = self
            .add_users
            .query_row(params![&entry.user_hash, &useragent_id], |row| {
                Ok(row.get_unwrap(0))
            })
            .unwrap();
        self.cache.users_cache.insert(key, id);
        id
    }

    pub fn add(&mut self, entry: &LogEntry) {
        let request_id = self.get_request_id(&entry);
        let user_id: i32 = self.get_user_id(&entry);
        self.add_entrys
            .execute(params![&entry.timestamp.timestamp(), &request_id, &user_id])
            .unwrap();
    }
}
