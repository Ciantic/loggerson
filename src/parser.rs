use crate::models::LogEntry;
use crate::models::Referrer;
use crate::models::Request;
use crate::models::User;
use crate::models::Useragent;
use md5::compute as md5;
use once_cell::sync::Lazy;
use regex::Regex;
use std::net::IpAddr;
use std::str::FromStr;

#[derive(Debug)]
pub struct ParseError(String);

impl ParseError {
    pub fn new(line: impl AsRef<str>) -> Self {
        ParseError(line.as_ref().to_owned())
    }
}

impl std::error::Error for ParseError {}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Unable to parse line '{}'", self.0)
    }
}

// https://httpd.apache.org/docs/2.4/logs.html
// (Looks like double quoted values need not escaping support?)
static COMBINED_LOG_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r#"^(?P<ip>[^ ]+) [^ ]+ [^ ]+ \[(?P<date>[^\]]+)\] "(?P<method>[^ "]+) (?P<url>[^ "]+) (?P<proto>[^ "]+)" (?P<status>\d+) (?P<bytes>\d+) "(?P<referrer>[^"]*)" "(?P<useragent>[^"]*)""#,
    ).unwrap()
});

pub fn parse(line: String) -> Result<LogEntry, ParseError> {
    if let Some(captures) = COMBINED_LOG_REGEX.captures(&line) {
        if let (
            Some(ipmatch),
            Some(datematch),
            Some(methodmatch),
            Some(urlmatch),
            // Some(_protomatch),
            Some(statusmatch),
            // Some(_bytesmatch),
            Some(referrermatch),
            Some(useragentmatch),
        ) = (
            captures.name("ip"),
            captures.name("date"),
            captures.name("method"),
            captures.name("url"),
            // captures.name("proto"),
            captures.name("status"),
            // captures.name("bytes"),
            captures.name("referrer"),
            captures.name("useragent"),
        ) {
            let ip = IpAddr::from_str(ipmatch.as_str()).map_err(|_| ParseError::new(&line))?;
            let dtime =
                chrono::DateTime::parse_from_str(datematch.as_str(), "%d/%b/%Y:%H:%M:%S %z")
                    .map_err(|_| ParseError::new(&line))?;
            let method = methodmatch.as_str().to_owned();
            let url = urlmatch.as_str().to_owned();
            let useragent = (useragentmatch.as_str() != "-").then(|| Useragent {
                value: useragentmatch.as_str().to_owned(),
            });
            let referrer = (referrermatch.as_str() != "-").then(|| Referrer {
                url: referrermatch.as_str().to_owned(),
            });
            let status_code = statusmatch
                .as_str()
                .parse::<i32>()
                .map_err(|_| ParseError::new(&line))?;

            // Truncate hash, and use bad hasher (which has known collisions
            // like md5), to make pin-pointing a user somewhat difficult. Since
            // the hash includes useragent there can be inifinite amount of
            // collisions if useragent list is cleaned periodically.
            let hash_bytes: [u8; 16] = md5(ip.to_string() + useragentmatch.as_str()).into();
            let mut hash_64b: [u8; 8] = [0; 8];
            hash_64b.copy_from_slice(&hash_bytes[0..8]);
            let hash = i64::from_le_bytes(hash_64b);

            Ok(LogEntry {
                timestamp: dtime.timestamp(),
                user: User {
                    hash: Some(hash),
                    useragent,
                },
                request: Request {
                    method,
                    status_code,
                    url,
                },
                referrer,
            })
        } else {
            // println!("Parsing row failed 1 {}", &line);
            Err(ParseError::new(line))
        }
    } else {
        // println!("Parsing row failed 2 {}", &line);
        Err(ParseError::new(line))
    }
}
