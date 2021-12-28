use derive_more::{From, Into};
use diesel::backend::Backend;
use diesel::expression::AsExpression;
use diesel::serialize::{self, Output, ToSql};
use diesel::sql_types::*;
use diesel::{
    backend::RawValue,
    deserialize::{self, FromSql},
};
use std::io::Write;
use std::{net::IpAddr, str::FromStr};

#[allow(unused_macros)]
macro_rules! generate_uuid_field {
    ( $name:ident ) => {
        #[derive(
            Debug,
            Clone,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Hash,
            serde::Serialize,
            serde::Deserialize,
            AsExpression,
            FromSqlRow,
        )]
        #[sql_type = "diesel::sql_types::Text"]
        pub struct $name(uuid::Uuid);

        impl $name {
            pub fn new() -> $name {
                $name(uuid::Uuid::new_v4())
            }

            pub fn from_uuid(uuid: uuid::Uuid) -> $name {
                $name(uuid)
            }
        }

        impl<DB> ToSql<diesel::sql_types::Text, DB> for $name
        where
            DB: Backend,
            String: ToSql<diesel::sql_types::Text, DB>,
        {
            fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, DB>) -> serialize::Result {
                <String as ToSql<diesel::sql_types::Text, DB>>::to_sql(
                    &self.0.to_hyphenated().to_string(),
                    out,
                )
            }
        }

        impl<DB> FromSql<diesel::sql_types::Text, DB> for $name
        where
            DB: Backend,
            String: FromSql<diesel::sql_types::Text, DB>,
        {
            fn from_sql(bytes: RawValue<DB>) -> deserialize::Result<Self> {
                let db_value_str =
                    <String as FromSql<diesel::sql_types::Text, DB>>::from_sql(bytes)?;
                let uuid_value = uuid::Uuid::parse_str(&db_value_str)?;
                Ok($name::from_uuid(uuid_value))
            }
        }

        // TODO: UUID FromSql, ToSql for other types, like binary or very long integer?
        // 1. Add diesel feature = "uuid",
        // 2. Create FromSql and ToSql for  diesel::pg::types::sql_types::Uuid
        //
        // More about: https://docs.diesel.rs/diesel/pg/types/sql_types/struct.Uuid.html
    };
}

macro_rules! generate_id_field {
    ( $name:ident ) => {
        #[derive(
            Debug,
            Copy,
            Clone,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Hash,
            serde::Serialize,
            serde::Deserialize,
            AsExpression,
            FromSqlRow,
            From,
            Into,
        )]
        #[sql_type = "diesel::sql_types::Integer"]
        pub struct $name(i32);

        impl $name {
            pub fn new(value: i32) -> $name {
                $name(value)
            }
        }

        impl<DB> ToSql<diesel::sql_types::Integer, DB> for $name
        where
            DB: Backend,
            i32: ToSql<diesel::sql_types::Integer, DB>,
        {
            fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, DB>) -> serialize::Result {
                <i32 as ToSql<diesel::sql_types::Integer, DB>>::to_sql(&self.0, out)
            }
        }

        impl<DB> FromSql<diesel::sql_types::Integer, DB> for $name
        where
            DB: Backend,
            i32: FromSql<diesel::sql_types::Integer, DB>,
        {
            fn from_sql(bytes: RawValue<DB>) -> deserialize::Result<Self> {
                let db_value_str = <i32>::from_sql(bytes)?;
                Ok($name(db_value_str))
            }
        }
    };
}

generate_id_field! {
    EntryId
}

generate_id_field! {
    RequestId
}

generate_id_field! {
    UserId
}

generate_id_field! {
    UserAgentId
}
