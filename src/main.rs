#[macro_use]
extern crate diesel;

#[macro_use]
extern crate diesel_migrations;

use crate::db::models::Request;
use db::{models::Entry, DbConnection};

mod db;

fn main() {
    let path = &".cache.db".into();
    let pool = DbConnection::new(path).unwrap();
    let a = pool.clone();

    let u = Request {
        id: None,
        method: "GET".into(),
        url: "https://example.com".into(),
        status_code: 200,
    };

    let u2 = u.save(&a).unwrap();

    println!("{:?}", u2);

    let e = Entry {
        id: None,
        timestamp: chrono::Local::now().timestamp(),
        request_id: u2,
        user_id: 0.into(),
    };

    e.save(&a).unwrap();
}
