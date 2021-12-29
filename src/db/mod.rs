use std::net::IpAddr;

use itertools::Itertools;
use r2d2::{Pool, PooledConnection};
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{params, Transaction};
use sha2::{Digest, Sha256};

use crate::LogEntry;

const SCHEMA: &str = include_str!("schema.sql");

fn create_schema(conn: &PooledConnection<SqliteConnectionManager>) -> Result<(), rusqlite::Error> {
    conn.execute_batch(SCHEMA)
}

pub fn init(path: &str) -> Result<Pool<SqliteConnectionManager>, rusqlite::Error> {
    let manager = SqliteConnectionManager::file(path);
    let pool = r2d2::Pool::new(manager).unwrap();
    let conn = pool.get().unwrap();
    create_schema(&conn)?;
    Ok(pool)
}

pub fn add(conn: &Transaction, entry: &LogEntry) -> i32 {
    let request_id: i32 = conn
        .query_row(
            "
            INSERT INTO 
            requests(method, url, status_code) 
            VALUES(?, ?, ?) 
            ON CONFLICT DO UPDATE SET id=id RETURNING id
            ",
            params![&entry.method, &entry.url, &entry.status],
            |row| Ok(row.get_unwrap(0)),
        )
        .unwrap();

    let useragent_id: i32 = conn
        .query_row(
            "
            INSERT INTO 
            useragents(value) 
            VALUES(?) 
            ON CONFLICT DO UPDATE SET id=id RETURNING id
            ",
            params![&entry.userAgent],
            |row| Ok(row.get_unwrap(0)),
        )
        .unwrap();

    let mut hasher = Sha256::new();
    hasher.update(&entry.ip.to_string());
    hasher.update(&entry.userAgent);
    let hash = hasher.finalize();

    let user_id: i32 = conn
        .query_row(
            "
            INSERT INTO
            users(hash, useragent_id)
            VALUES(?, ?)
            ON CONFLICT DO UPDATE SET id=id RETURNING id
            ",
            params![&hash.as_slice(), &useragent_id],
            |row| Ok(row.get_unwrap(0)),
        )
        .unwrap();

    let entry_id: i32 = conn
        .query_row(
            "
        INSERT INTO
        entrys(timestamp, request_id, user_id)
        VALUES(?, ?, ?)
        ON CONFLICT DO UPDATE SET id=id RETURNING id
        ",
            params![&entry.timestamp.timestamp(), &request_id, &user_id],
            |row| Ok(row.get_unwrap(0)),
        )
        .unwrap();

    return entry_id;
}
