use itertools::Either;
use itertools::Itertools;
use once_cell::sync::Lazy;
use rayon::prelude::ParallelIterator;
use rayon::prelude::*;
use regex::Regex;
use sha2::{Digest, Sha256};
use std::fmt::Write;
use std::io::BufRead;
use std::{fs::File, io, net::IpAddr, path::Path, str::FromStr, time::Instant};

use crate::models::LogEntry;
use crate::models::Request;
use crate::models::User;
use crate::models::Useragent;

#[derive(Debug)]
pub enum ParseError {
    IoError(std::io::Error),
    ParsingError,
}

// https://httpd.apache.org/docs/2.4/logs.html
// (Looks like double quoted values need not escaping support?)
static COMBINED_LOG_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r#"^(?P<ip>[^ ]+) [^ ]+ [^ ]+ \[(?P<date>[^\]]+)\] "(?P<method>[^ "]+) (?P<url>[^ "]+) (?P<proto>[^ "]+)" (?P<status>\d+) (?P<bytes>\d+) "(?P<referrer>[^"]*)" "(?P<useragent>[^"]*)""#,
    ).unwrap()
});

pub fn parse(line: String) -> Result<LogEntry, ParseError> {
    use ParseError::*;
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
            let ip = IpAddr::from_str(ipmatch.as_str()).map_err(|_| ParsingError)?;
            let dtime =
                chrono::DateTime::parse_from_str(datematch.as_str(), "%d/%b/%Y:%H:%M:%S %z")
                    .map_err(|_| ParsingError)?;
            let method = methodmatch.as_str();
            let url = urlmatch.as_str();
            let useragent = useragentmatch.as_str();
            let referrer = referrermatch.as_str();
            let status = statusmatch
                .as_str()
                .parse::<i32>()
                .map_err(|_| ParsingError)?;

            let mut hasher = Sha256::new();
            hasher.update(ip.to_string());
            hasher.update(&useragent);
            let hashbytes = hasher.finalize();
            let mut hash = String::with_capacity(32);
            for &b in hashbytes.as_slice() {
                write!(&mut hash, "{:02x}", b).unwrap();
            }

            /*
            let mut hash = [0u8; 32];
            hash.copy_from_slice(&hashbytes);
            */

            Ok(LogEntry {
                timestamp: dtime.timestamp(),
                user: User {
                    hash: Some(hash),
                    useragent: Some(Useragent {
                        value: useragent.to_owned(),
                    }),
                },
                request: Request {
                    method: method.to_owned(),
                    status_code: status,
                    url: url.to_owned(),
                },
            })
        } else {
            // println!("Parsing row failed 1 {}", &line);
            Err(ParsingError)
        }
    } else {
        // println!("Parsing row failed 2 {}", &line);
        Err(ParsingError)
    }
}
