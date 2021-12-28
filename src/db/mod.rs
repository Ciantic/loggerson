pub mod models;
pub mod repositories;
pub mod schema;
pub mod types;

use derive_more::From;

use diesel::prelude::*;
use diesel::{
    backend::Backend, deserialize, r2d2::ConnectionManager, serialize, serialize::Output,
    types::FromSql, types::ToSql, AsExpression, SqliteConnection,
};
use diesel_migrations::embed_migrations;
use r2d2::{Pool, PooledConnection};
use std::{io::Write, path::PathBuf};

#[derive(Debug, From)]
pub enum Error {
    NotFound,
    MigrationError,
    ConnectionError,
    OtherDbError(diesel::result::Error),
}

pub type DbResult<T> = Result<T, Error>;

#[derive(Clone)]
pub struct DbConnection {
    pool: Pool<ConnectionManager<SqliteConnection>>,
}

embed_migrations!("migrations");

impl DbConnection {
    pub fn new(database_path: &PathBuf) -> Result<Self, Error> {
        let db_url = &database_path.to_string_lossy().into_owned();
        // Ok(DbConnection::new_from_url(db_url).await?)
        Ok(DbConnection::new_from_url(db_url)?)
    }

    pub fn new_from_url(database_url: &str) -> Result<Self, Error> {
        let conman = ConnectionManager::<SqliteConnection>::new(database_url);
        let pool = Pool::builder()
            .max_size(15)
            .build(conman)
            .map_err(|_er| Error::ConnectionError)?;
        embedded_migrations::run_with_output(&pool.get().unwrap(), &mut std::io::stdout())
            .map_err(|_er| Error::MigrationError)?;
        Ok(DbConnection { pool })
    }
    // pub fn new(pool: Pool<ConnectionManager<SqliteConnection>>) -> Self {
    //     DbConnection { pool }
    // }

    pub fn get(&self) -> DbResult<PooledConnection<ConnectionManager<SqliteConnection>>> {
        let c: PooledConnection<ConnectionManager<SqliteConnection>> = self
            .pool
            .get_timeout(std::time::Duration::from_secs(12))
            .map_err(|_| Error::ConnectionError)?;
        Ok(c)
    }
}

// unsafe impl Send for DbConnection {}
// unsafe impl Sync for DbConnection {}
