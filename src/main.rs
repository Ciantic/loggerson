use chrono::{DateTime, FixedOffset};
use itertools::Either;
use itertools::Itertools;
use once_cell::sync::Lazy;
use once_cell::sync::OnceCell;
use rayon::prelude::ParallelIterator;
use rayon::prelude::*;
use regex::Regex;
use sha2::{Digest, Sha256};
use std::fmt::Write;
use std::io::BufRead;
use std::{fs::File, io, net::IpAddr, path::Path, str::FromStr, time::Instant};

use crate::db::{init, BatchCache, BatchInsertor};

mod db;

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

#[derive(PartialEq, Eq, Clone, Hash, Debug)]
pub struct Request {
    method: String,
    url: String,
    status_code: i32,
}

#[derive(PartialEq, Eq, Clone, Hash, Debug)]
pub struct User {
    hash: Option<String>,
    useragent: Option<Useragent>,
    // TODO: Country struct
}

#[derive(PartialEq, Eq, Clone, Hash, Debug)]
pub struct Useragent {
    value: String,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct LogEntry {
    timestamp: i64,
    request: Request,
    user: User,
    // TODO: Referrer struct
}

impl LogEntry {
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
}

fn main() {
    // let pool = init(".cache.db").unwrap();
    let conpool = init(".cache.db").unwrap();

    // e.save(&mut a).unwrap();
    let lines = read_lines(".cache/access_log").unwrap();

    // let par = lines.chunks(100000);
    let par = lines.chunks(100000);
    let mut cache = BatchCache::new();
    let mut all_entries = 0;
    let stuff =
        par.into_iter()
            .enumerate()
            .map(|(chunk_n, chunkedlines)| -> Result<(), ParseError> {
                let start_of_chunk_time = Instant::now();
                let mut lines = chunkedlines.collect::<Vec<_>>();

                // Parse all rows in parllel
                println!("Parsing a chunk {} sized {}...", chunk_n + 1, lines.len());
                let parse_results = lines
                    .par_drain(..)
                    .map(|lineresult| LogEntry::parse(lineresult.unwrap()));

                // Separate failures and successes
                let (mut entries, failures): (Vec<LogEntry>, Vec<ParseError>) = parse_results
                    .partition_map(|v| match v {
                        Ok(v) => Either::Left(v),
                        Err(e) => Either::Right(e),
                    });

                // Sort by timestamp
                entries.par_sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

                all_entries += entries.len();

                println!(
                    "Parsed a chunk in {} ms, successes {}, failures {}.",
                    (Instant::now() - start_of_chunk_time).as_millis(),
                    entries.len(),
                    failures.len()
                );

                let first = entries.first().unwrap();
                let last = entries.last().unwrap();
                println!(
                    "Inserting to database all chunks between {} and {}...",
                    first.timestamp, last.timestamp
                );

                let mut conn = conpool.get().unwrap();
                let tx = conn.transaction().unwrap();
                {
                    let mut insertor = BatchInsertor::new(&tx, &mut cache);
                    insertor.add_entries(&entries);
                }
                tx.commit().unwrap();
                Ok(())
            });

    stuff.for_each(|r| {});

    println!("All entries {}", all_entries);
}

// The output is wrapped in a Result to allow matching on errors
// Returns an Iterator to the Reader of the lines of the file.
fn read_lines<P>(filename: P) -> std::io::Result<std::io::Lines<std::io::BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}
