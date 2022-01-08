#[derive(PartialEq, Eq, Clone, Hash, Debug)]
pub struct Request {
    pub method: String,
    pub url: String,
    pub status_code: i32,
}

#[derive(PartialEq, Eq, Clone, Hash, Debug)]
pub struct User {
    pub hash: Option<String>,
    pub useragent: Option<Useragent>,
    // TODO: Country struct
}

#[derive(PartialEq, Eq, Clone, Hash, Debug)]
pub struct Useragent {
    pub value: String,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct LogEntry {
    pub timestamp: i64,
    pub request: Request,
    pub user: User,
    pub referrer: Option<Referrer>,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Referrer {
    pub url: String,
}
