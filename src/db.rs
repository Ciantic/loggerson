use crate::{
    iterutils::{ExtendTo, TransmitErrorsExt},
    models::{LogEntry, Request, User, Useragent},
};
use itertools::Itertools;
use r2d2::{Pool, PooledConnection};
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{params, Connection};
use std::collections::HashMap;

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

    pub fn populate(
        &mut self,
        con: &Connection,
        error_channel: &std::sync::mpsc::Sender<rusqlite::Error>,
    ) -> rusqlite::Result<()> {
        {
            // Update requests cache
            let mut stmt = con.prepare_cached(
                "
                SELECT r.id, r.method, r.url, r.status_code FROM requests r
            ",
            )?;

            stmt.query([])?
                .mapped(|row| {
                    Ok((
                        Request {
                            method: row.get(1)?,
                            url: row.get(2)?,
                            status_code: row.get(3)?,
                        },
                        row.get(0)?,
                    ))
                })
                .transmit_errors(&error_channel)
                .extend_to(&mut self.requests_cache);
        }

        println!("Update users cache");
        {
            // Update users cache
            let mut stmt = con.prepare_cached(
                "
                SELECT 
                    DISTINCT 
                    u.id as user_id, 
                    u.hash as user_hash, 
                    ua.value as useragent_value
                FROM users u, useragents ua 
                WHERE u.useragent_id = ua.id 
                ",
            )?;

            stmt.query([])?
                .mapped(|row| {
                    Ok((
                        User {
                            hash: row.get(1)?,
                            useragent: Some(Useragent { value: row.get(2)? }),
                        },
                        row.get(0)?,
                    ))
                })
                .transmit_errors(&error_channel)
                .extend_to(&mut self.users_cache);
        }

        println!("Update useragents");
        {
            // Update useragents cache
            let mut stmt = con.prepare_cached(
                "
                SELECT 
                    ua.id,
                    ua.value
                FROM useragents ua
                ",
            )?;

            stmt.query([])?
                .mapped(|row| Ok((Useragent { value: row.get(1)? }, row.get(0)?)))
                .transmit_errors(&error_channel)
                .extend_to(&mut self.useragents_cache);
        }
        Ok(())
    }
}

pub fn batch_insert(
    error_channel: &std::sync::mpsc::Sender<rusqlite::Error>,
    con: &Connection,
    entries: &Vec<LogEntry>,
    caches: &mut BatchCache,
) -> rusqlite::Result<()> {
    println!("Insert requests");
    {
        // Insert requests, update cache
        let mut stmt = con.prepare_cached(
            "
                INSERT INTO 
                requests(method, url, status_code) 
                VALUES(?, ?, ?)
                RETURNING id
                ",
        )?;

        entries
            .iter()
            .map(|e| &e.request)
            .filter(|r| !caches.requests_cache.contains_key(r))
            .unique()
            .map(|row| -> rusqlite::Result<_> {
                Ok((
                    row.clone(),
                    stmt.query_row(
                        params![row.method, row.url, row.status_code],
                        // Get the ID
                        |row| Ok(row.get(0)?),
                    )?,
                ))
            })
            .transmit_errors(&error_channel)
            .collect_vec()
            .extend_to(&mut caches.requests_cache);
    }

    println!("Insert useragents");
    {
        // Insert useragents, update cache
        let mut stmt = con.prepare_cached(
            "
                INSERT INTO 
                useragents(value) 
                VALUES(?) 
                RETURNING id
                ",
        )?;

        entries
            .iter()
            .filter_map(|e| e.user.useragent.to_owned())
            .unique()
            .filter(|r| !caches.useragents_cache.contains_key(r))
            .map(|useragent| -> rusqlite::Result<_> {
                Ok((
                    useragent.clone(),
                    stmt.query_row(
                        params![useragent.value],
                        // Get id
                        |row| Ok(row.get(0)?),
                    )?,
                ))
            })
            .transmit_errors(&error_channel)
            .collect_vec()
            .extend_to(&mut caches.useragents_cache);
    }

    println!("Insert users");
    {
        // Insert users, update cache
        let mut stmt = con.prepare_cached(
            "
                INSERT INTO
                users(hash, useragent_id)
                VALUES(?, ?)
                RETURNING id
                ",
        )?;

        entries
            .iter()
            .filter(|r| !caches.users_cache.contains_key(&r.user))
            .filter_map(|entry| -> Option<_> {
                // TODO: Convert these to errors
                let useragent = &entry.user.useragent.clone().unwrap();
                let useragent_id = caches.useragents_cache.get(useragent).unwrap();
                let hash = entry.user.hash.clone().unwrap();

                Some((&entry.user, useragent_id, hash))
            })
            .unique()
            .map(|(user, useragent_id, hash)| {
                Ok((
                    user.clone(),
                    stmt.query_row(
                        params![hash, useragent_id],
                        // Get the id
                        |row| Ok(row.get(0)?),
                    )?,
                ))
            })
            .transmit_errors(&error_channel)
            .collect_vec()
            .extend_to(&mut caches.users_cache);
    }

    println!("Insert entries");
    {
        // Insert entries
        let mut stmt = con.prepare_cached(
            "
                INSERT INTO
                entrys(timestamp, request_id, user_id)
                VALUES(?, ?, ?)
                ON CONFLICT DO NOTHING
                ",
        )?;

        entries
            .iter()
            .filter_map(|entry| {
                // TODO: Convert these to errors
                let request_id = caches.requests_cache.get(&entry.request).unwrap();
                let user_id = caches.users_cache.get(&entry.user).unwrap();
                Some((entry, request_id, user_id))
            })
            .map(|(entry, request_id, user_id)| {
                stmt.execute(params![entry.timestamp, request_id, user_id])
            })
            .transmit_errors(&error_channel)
            .for_each(drop);
    }
    Ok(())
}
