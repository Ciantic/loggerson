table! {
    entrys (id) {
        id -> Nullable<Integer>,
        timestamp -> BigInt,
        request_id -> Integer,
        user_id -> Integer,
    }
}

table! {
    requests (id) {
        id -> Nullable<Integer>,
        method -> Text,
        url -> Text,
        status_code -> Integer,
    }
}

table! {
    useragents (id) {
        id -> Nullable<Integer>,
        value -> Text,
    }
}

table! {
    users (id) {
        id -> Nullable<Integer>,
        hash -> Text,
        useragent_id -> Integer,
    }
}

joinable!(entrys -> requests (request_id));
joinable!(entrys -> users (user_id));
joinable!(users -> useragents (useragent_id));

allow_tables_to_appear_in_same_query!(
    entrys,
    requests,
    useragents,
    users,
);
