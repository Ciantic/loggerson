use crate::{
    iterutils::{ExtendTo, TransmitErrors},
    models::{LogEntry, Request, User, Useragent},
};
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
    useragents_cache: HashMap<Useragent, i32>,
    users_cache: HashMap<User, i32>,
    requests_cache: HashMap<Request, i32>,
}

impl BatchCache {
    pub fn new() -> Self {
        BatchCache {
            useragents_cache: HashMap::new(),
            users_cache: HashMap::new(),
            requests_cache: HashMap::new(),
        }
    }
}

pub fn batch_insert(
    con: &Connection,
    entries: &Vec<LogEntry>,
    caches: &mut BatchCache,
) -> rusqlite::Result<()> {
    let first = entries.first().unwrap().timestamp;
    let last = entries.last().unwrap().timestamp;

    {
        // Update requests cache
        let mut stmt = con.prepare_cached(
            "
                SELECT DISTINCT r.id, r.method, r.url, r.status_code
                FROM entrys e, requests r
                WHERE e.request_id = r.id
                AND e.timestamp >= ? AND e.timestamp <= ?
            ",
        )?;

        stmt.query(params![first, last])?
            .mapped(|row| {
                Ok((
                    Request {
                        method: row.get(1)?,
                        url: row.get(2)?,
                        status_code: row.get(3)?,
                    },
                    row.get::<_, i32>(0)?,
                ))
            })
            .transmit_errors(15)
            .extend_to(&mut caches.requests_cache);
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
                FROM entrys e, users u, useragents ua 
                WHERE 
                    e.user_id = u.id AND u.useragent_id = ua.id 
                    AND e.timestamp >= ? AND e.timestamp <= ?
                ",
        )?;

        stmt.query(params![first, last])?
            .mapped(|row| {
                Ok((
                    User {
                        hash: row.get(1)?,
                        useragent: Some(Useragent { value: row.get(2)? }),
                    },
                    row.get::<_, i32>(0)?,
                ))
            })
            .transmit_errors(15)
            .extend_to(&mut caches.users_cache);
    }

    {
        // Update useragents cache
        let mut stmt = con.prepare_cached(
            "
                SELECT 
                    DISTINCT 
                    ua.id as useragent_id
                    ua.value as useragent_value
                FROM entrys e, users u, useragents ua 
                WHERE 
                    e.user_id = u.id AND u.useragent_id = ua.id 
                    AND e.timestamp >= ? AND e.timestamp <= ?
                ",
        )?;

        let rows: Vec<_> = Result::from_iter(stmt.query_map(params![first, last], |row| {
            Ok((Useragent { value: row.get(1)? }, row.get::<_, i32>(0)?))
        })?)?;

        caches.useragents_cache.extend(rows);
    }

    {
        // Insert requests, update cache
        let mut stmt = con.prepare_cached(
            "
                INSERT INTO 
                requests(method, url, status_code) 
                VALUES(?, ?, ?)
                ON CONFLICT DO UPDATE SET id=id
                RETURNING id
                ",
        )?;
        let rows: Vec<_> = Result::from_iter(entries.iter().map(|e| &e.request).map(
            |row| -> rusqlite::Result<_> {
                Ok((
                    row.clone(),
                    stmt.query_row(
                        params![row.method, row.url, row.status_code],
                        // Get the id
                        |row| Ok(row.get::<_, i32>(0)?),
                    )?,
                ))
            },
        ))?;
        caches.requests_cache.extend(rows);
    }

    {
        // Insert useragents, update cache
        let mut stmt = con
            .prepare_cached(
                "
                INSERT INTO 
                useragents(value) 
                VALUES(?) 
                ON CONFLICT DO UPDATE SET id=id
                RETURNING id
                ",
            )
            .unwrap();

        let rows: Vec<_> =
            Result::from_iter(entries.iter().filter_map(|e| e.user.useragent.clone()).map(
                |useragent| -> rusqlite::Result<_> {
                    Ok((
                        useragent.clone(),
                        stmt.query_row(params![useragent.value], |row| Ok(row.get::<_, i32>(0)?))?,
                    ))
                },
            ))?;

        caches.useragents_cache.extend(rows);
    }

    {
        // Insert users, update cache
        let mut stmt = con
            .prepare_cached(
                "
                INSERT INTO
                users(hash, useragent_id)
                VALUES(?, ?)
                ON CONFLICT DO UPDATE SET id=id
                RETURNING id
                ",
            )
            .unwrap();
        let rows: Vec<_> = Result::from_iter(
            entries
                .iter()
                .map(|f| {
                    (
                        caches
                            .useragents_cache
                            .get(&f.user.useragent.clone().unwrap()),
                        &f.user,
                    )
                })
                .filter_map(|(useragent_id, user)| {
                    Some((user.clone(), user.clone().hash?, useragent_id?))
                })
                .map(|(user, hash, useragent_id)| -> rusqlite::Result<_> {
                    Ok((
                        user.clone(),
                        stmt.query_row(params![hash, useragent_id], |row| {
                            Ok(row.get_unwrap::<_, i32>(0))
                        })?,
                    ))
                }),
        )?;
        caches.users_cache.extend(rows);
    }

    {
        // Insert entries
        let mut stmt = con
            .prepare_cached(
                "
                INSERT INTO
                entrys(timestamp, request_id, user_id)
                VALUES(?, ?, ?)
                ON CONFLICT DO NOTHING
                ",
            )
            .unwrap();

        for i in entries {
            let request_id = caches.requests_cache.get(&i.request).unwrap();
            let user_id = caches.users_cache.get(&i.user).unwrap();
            stmt.execute(params![i.timestamp, request_id, user_id])?;
        }
    }
    Ok(())
}
