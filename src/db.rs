use crate::{
    models::{LogEntry, Request, User, Useragent},
    utils::{ExtendTo, SendErrorsExt},
    DiagMsg, Error,
};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{params, Connection};
use std::collections::HashMap;

const SCHEMA: &str = include_str!("schema.sql");

pub fn init(path: &str) -> Result<Pool<SqliteConnectionManager>, rusqlite::Error> {
    // let manager = SqliteConnectionManager::memory();
    let manager = SqliteConnectionManager::file(path);
    let pool = r2d2::Pool::new(manager).unwrap();
    let conn = pool.get().unwrap();
    conn.execute_batch(SCHEMA)?;
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
        error_channel: &crossbeam_channel::Sender<DiagMsg>,
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
                .map(|res| res.map_err(Error::SqliteError))
                .send_errors(&error_channel)
                .extend_to(&mut self.requests_cache);
        }

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
                .map(|res| res.map_err(Error::SqliteError))
                .send_errors(&error_channel)
                .extend_to(&mut self.users_cache);
        }

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
                .map(|res| res.map_err(Error::SqliteError))
                .send_errors(&error_channel)
                .extend_to(&mut self.useragents_cache);
        }
        Ok(())
    }
}

fn insert_request(
    caches: &mut BatchCache,
    con: &Connection,
    request: &Request,
) -> rusqlite::Result<i32> {
    if let Some(request_id) = caches.requests_cache.get(request) {
        return Ok(request_id.to_owned());
    }
    let mut stmt = con.prepare_cached(
        "
            INSERT INTO 
            requests(method, url, status_code) 
            VALUES(?, ?, ?)
            RETURNING id
            ",
    )?;
    let request_id = stmt.query_row(
        params![request.method, request.url, request.status_code],
        // Get the ID
        |row| Ok(row.get(0)?),
    )?;
    caches.requests_cache.insert(request.to_owned(), request_id);
    Ok(request_id)
}

fn inser_useragent(
    caches: &mut BatchCache,
    con: &Connection,
    object: &Useragent,
) -> rusqlite::Result<i32> {
    if let Some(request_id) = caches.useragents_cache.get(object) {
        return Ok(request_id.to_owned());
    }
    let mut stmt = con.prepare_cached(
        "
            INSERT INTO 
            useragents(value) 
            VALUES(?) 
            RETURNING id
        ",
    )?;
    let request_id = stmt.query_row(
        params![object.value],
        // Get the ID
        |row| Ok(row.get(0)?),
    )?;
    caches
        .useragents_cache
        .insert(object.to_owned(), request_id);
    Ok(request_id)
}

fn insert_user(caches: &mut BatchCache, con: &Connection, object: &User) -> rusqlite::Result<i32> {
    if let Some(request_id) = caches.users_cache.get(object) {
        return Ok(request_id.to_owned());
    }
    // TODO: Remove unwrap
    let useragent_id = inser_useragent(caches, &con, &object.useragent.clone().unwrap())?;
    let mut stmt = con.prepare_cached(
        "
            INSERT INTO
            users(hash, useragent_id)
            VALUES(?, ?)
            RETURNING id
        ",
    )?;
    let request_id = stmt.query_row(
        params![object.hash, useragent_id],
        // Get the ID
        |row| Ok(row.get(0)?),
    )?;
    caches.users_cache.insert(object.to_owned(), request_id);
    Ok(request_id)
}

fn insert_entry(
    caches: &mut BatchCache,
    con: &Connection,
    object: &LogEntry,
) -> rusqlite::Result<()> {
    let request_id = insert_request(caches, &con, &object.request)?;
    let user_id = insert_user(caches, &con, &object.user)?;
    let mut stmt = con.prepare_cached(
        "
            INSERT INTO
            entrys(timestamp, request_id, user_id)
            VALUES(?, ?, ?)
            ON CONFLICT DO NOTHING
            ",
    )?;
    stmt.execute(params![object.timestamp, request_id, user_id])?;

    Ok(())
}

pub fn batch_insert(
    diag_channel: &crossbeam_channel::Sender<DiagMsg>,
    con: &Connection,
    entries: &Vec<LogEntry>,
    caches: &mut BatchCache,
) -> rusqlite::Result<()> {
    entries
        .iter()
        .map(|entry| insert_entry(caches, &con, entry))
        .map(|res| res.map_err(Error::SqliteError))
        .send_errors(&diag_channel)
        .for_each(|_| diag_channel.send(DiagMsg::RowInserted).unwrap());
    Ok(())
}
