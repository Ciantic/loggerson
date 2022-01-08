use crate::{
    models::{LogEntry, Referrer, Request, User, Useragent},
    utils::{ExtendTo, SendErrorsExt},
    Error, Msg,
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
    // let _ = conn
    //     .query_row("PRAGMA journal_mode = WAL", [], |_row| Ok(()))
    //     .unwrap();
    conn.execute_batch(SCHEMA)?;
    Ok(pool)
}
pub struct BatchCache {
    pub useragents_cache: HashMap<Useragent, i32>,
    pub users_cache: HashMap<User, i32>,
    pub requests_cache: HashMap<Request, i32>,
    pub referrer_cache: HashMap<Referrer, i32>,
}

impl BatchCache {
    pub fn new() -> Self {
        BatchCache {
            useragents_cache: HashMap::new(),
            users_cache: HashMap::new(),
            requests_cache: HashMap::new(),
            referrer_cache: HashMap::new(),
        }
    }

    pub fn populate(
        &mut self,
        con: &Connection,
        error_channel: &crossbeam_channel::Sender<Msg>,
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

        {
            // Update referrer cache
            let mut stmt = con.prepare_cached(
                "
                SELECT 
                    r.id,
                    r.url
                FROM referrers r
                ",
            )?;

            stmt.query([])?
                .mapped(|row| Ok((Referrer { url: row.get(1)? }, row.get(0)?)))
                .map(|res| res.map_err(Error::SqliteError))
                .send_errors(&error_channel)
                .extend_to(&mut self.referrer_cache);
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

fn insert_referrer(
    caches: &mut BatchCache,
    con: &Connection,
    referrer: &Referrer,
) -> rusqlite::Result<i32> {
    if let Some(request_id) = caches.referrer_cache.get(referrer) {
        return Ok(request_id.to_owned());
    }
    let mut stmt = con.prepare_cached(
        "
            INSERT INTO 
            referrers(url) 
            VALUES(?)
            RETURNING id
            ",
    )?;
    let referrer_id = stmt.query_row(
        params![referrer.url],
        // Get the ID
        |row| Ok(row.get(0)?),
    )?;
    caches
        .referrer_cache
        .insert(referrer.to_owned(), referrer_id);
    Ok(referrer_id)
}

fn insert_entry(
    caches: &mut BatchCache,
    con: &Connection,
    object: &LogEntry,
) -> rusqlite::Result<()> {
    let request_id = insert_request(caches, &con, &object.request)?;
    let user_id = insert_user(caches, &con, &object.user)?;
    let referrer_id = object
        .referrer
        .as_ref()
        .map(|r| insert_referrer(caches, &con, &r))
        .transpose()?;

    let mut stmt = con.prepare_cached(
        "
            INSERT INTO
            entrys(timestamp, request_id, user_id, referrer_id)
            VALUES(?, ?, ?, ?)
            ON CONFLICT DO NOTHING
            ",
    )?;
    stmt.execute(params![object.timestamp, request_id, user_id, referrer_id])?;

    Ok(())
}

pub fn batch_insert(
    msg_sender: &crossbeam_channel::Sender<Msg>,
    con: &Connection,
    entries: &Vec<LogEntry>,
    caches: &mut BatchCache,
) -> rusqlite::Result<()> {
    entries
        .iter()
        .map(|entry| insert_entry(caches, &con, entry))
        .map(|res| res.map_err(Error::SqliteError))
        .send_errors(&msg_sender)
        .for_each(|_| msg_sender.send(Msg::RowInserted).unwrap());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{init, insert_entry, BatchCache};
    use crate::models::*;
    use itertools::Itertools;

    #[test]
    fn test_init_schema() {
        let _ = init(":memory:").unwrap();
    }

    #[test]
    fn test_insert_entry() {
        let con = init(":memory:").unwrap().get().unwrap();
        let mut caches = BatchCache::new();
        insert_entry(
            &mut caches,
            &con,
            &LogEntry {
                timestamp: 100,
                request: Request {
                    method: "GET".to_owned(),
                    url: "https://example.com".to_owned(),
                    status_code: 300,
                },
                user: User {
                    hash: Some("123".to_owned()),
                    useragent: Some(Useragent {
                        value: "Foo".to_owned(),
                    }),
                },
                referrer: Some(Referrer {
                    url: "https://test".to_owned(),
                }),
            },
        )
        .unwrap();

        let mut stmt = con
            .prepare(
                "
                SELECT e.timestamp, u.hash, r.method, r.url, r.status_code, ua.value as useragent, rr.url as referrer_url FROM 
                entrys e, 
                requests r, 
                users u, 
                useragents ua, 
                referrers rr
                WHERE
                e.request_id = r.id AND
                e.user_id = u.id AND
                e.referrer_id = rr.id AND
                u.useragent_id = ua.id
            ",
            )
            .unwrap();

        let rows = stmt
            .query_map([], |f| {
                let values: (i64, String, String, String, i32, String, String) = (
                    f.get(0)?,
                    f.get(1)?,
                    f.get(2)?,
                    f.get(3)?,
                    f.get(4)?,
                    f.get(5)?,
                    f.get(6)?,
                );
                Ok(values)
            })
            .unwrap()
            .flatten()
            .collect_vec();
        assert_eq!(
            vec![(
                100,
                "123".to_owned(),
                "GET".to_owned(),
                "https://example.com".to_owned(),
                300,
                "Foo".to_owned(),
                "https://test".to_owned()
            )],
            rows
        );
    }
}
