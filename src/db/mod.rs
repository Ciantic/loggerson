use std::net::IpAddr;

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

pub struct BatchInsertor<'conn> {
    add_requests: CachedStatement<'conn>,
    add_useragents: CachedStatement<'conn>,
    add_users: CachedStatement<'conn>,
    add_entrys: CachedStatement<'conn>,
}

unsafe impl<'conn> Sync for BatchInsertor<'conn> {}
unsafe impl<'conn> Send for BatchInsertor<'conn> {}

impl<'conn> BatchInsertor<'conn> {
    pub fn new(conn: &'conn Transaction) -> BatchInsertor<'conn> {
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
                ON CONFLICT DO UPDATE SET id=id RETURNING id
            ",
            )
            .unwrap();

        BatchInsertor {
            add_entrys,
            add_requests,
            add_useragents,
            add_users,
        }
    }

    pub fn add(&mut self, entry: &LogEntry) -> i32 {
        let request_id: i32 = self
            .add_requests
            .query_row(params![&entry.method, &entry.url, &entry.status], |row| {
                Ok(row.get_unwrap(0))
            })
            .unwrap();

        let useragent_id: i32 = self
            .add_useragents
            .query_row(params![&entry.useragent], |row| Ok(row.get_unwrap(0)))
            .unwrap();

        let user_id: i32 = self
            .add_users
            .query_row(params![&entry.hash, &useragent_id], |row| {
                Ok(row.get_unwrap(0))
            })
            .unwrap();

        let entry_id: i32 = self
            .add_entrys
            .query_row(
                params![&entry.timestamp.timestamp(), &request_id, &user_id],
                |row| Ok(row.get_unwrap(0)),
            )
            .unwrap();

        return entry_id;
    }
}
