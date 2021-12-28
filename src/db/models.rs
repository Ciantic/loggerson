use super::types::*;
use crate::db::schema::*;
use chrono::NaiveDateTime;

#[derive(
    Debug,
    Queryable,
    Identifiable,
    Insertable,
    AsChangeset,
    // Associations,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct Entry {
    pub id: Option<EntryId>,
    pub timestamp: i64,
    pub request_id: RequestId,
    pub user_id: UserId,
}

#[derive(
    Debug,
    Queryable,
    Identifiable,
    Insertable,
    AsChangeset,
    // Associations,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct Request {
    pub id: Option<RequestId>,
    pub method: String,
    pub url: String,
    pub status_code: i32,
}

#[derive(
    Debug,
    Queryable,
    Identifiable,
    Insertable,
    AsChangeset,
    // Associations,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct User {
    pub id: Option<UserId>,
    pub hash: String,
    pub useragent_id: UserAgentId,
}

#[derive(
    Debug,
    Queryable,
    Identifiable,
    Insertable,
    AsChangeset,
    // Associations,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct Useragent {
    pub id: Option<UserAgentId>,
    pub value: String,
}
