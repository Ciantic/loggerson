use chrono::{DateTime, FixedOffset};
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

pub struct LogEntry {
    pub user_hash: String,
    pub timestamp: DateTime<FixedOffset>,
    pub method: String,
    pub url: String,
    pub status: i32,
    pub useragent: String,
    pub referrer: String,
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
                // println!("hash {}", hash);

                Ok(LogEntry {
                    user_hash: hash,
                    timestamp: dtime,
                    method: method.to_owned(),
                    url: url.to_owned(),
                    referrer: referrer.to_owned(),
                    status: status,
                    useragent: useragent.to_owned(),
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

    let par = lines.chunks(100000);
    let mut cache = BatchCache::new();
    let stuff = par
        .into_iter()
        .map(|chunkedlines| -> Result<(), ParseError> {
            let start_of_chunk_time = Instant::now();
            let mut lines = chunkedlines.collect::<Vec<_>>();

            println!("Start parsing a chunk sized {}...", lines.len());

            let entriespar =
                lines
                    .par_drain(..)
                    .map(|lineresult| -> Result<LogEntry, ParseError> {
                        LogEntry::parse(lineresult.unwrap())
                    });

            let (mut entries, failures): (Vec<LogEntry>, Vec<ParseError>) = entriespar
                .partition_map(|v| match v {
                    Ok(v) => Either::Left(v),
                    Err(e) => Either::Right(e),
                });

            entries.par_sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

            println!(
                "Parsed a chunk in {} ms, successes {}, failures {}.",
                (Instant::now() - start_of_chunk_time).as_millis(),
                entries.len(),
                failures.len()
            );

            println!("Inserting chunk to a database...");

            let mut con = conpool.clone().get().unwrap();
            let tx = con.transaction().unwrap();
            {
                if let (Some(first), Some(last)) = (entries.first(), entries.last()) {
                    println!("Timespan: {} to {}", first.timestamp, last.timestamp);
                }

                println!(
                    "Cache? {} {} {}",
                    &cache.requests_cache.len(),
                    &cache.users_cache.len(),
                    &cache.useragents_cache.len()
                );
                let mut insertor = BatchInsertor::new(&tx, &mut cache);

                entries.iter().enumerate().for_each(|(n, r)| {
                    if n % 1000 == 0 {
                        println!("Sqlite add calls {}", n);
                    }
                    insertor.add(&r);
                });
                println!("ADDED A CHUNK");
            }
            tx.commit().unwrap();
            println!("COMMITED A CHUNK");

            Ok(())
        });

    stuff.for_each(|r| {});
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
