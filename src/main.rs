use std::{borrow::BorrowMut, fs::File, io, net::IpAddr, path::Path, str::FromStr};

use chrono::{DateTime, FixedOffset};
use regex::Regex;
use sha2::{Digest, Sha256};
use std::io::BufRead;

use crate::db::{init, BatchInsertor};

mod db;

#[derive(Debug)]
enum ParseError {
    IoError(std::io::Error),
    ParsingError,
}

pub struct LogEntry {
    pub hash: Vec<u8>,
    pub timestamp: DateTime<FixedOffset>,
    pub method: String,
    pub url: String,
    pub status: i32,
    pub useragent: String,
    pub referrer: String,
}

fn main() {
    // let pool = init(".cache.db").unwrap();
    let pool = init(".cache.db").unwrap();

    // https://httpd.apache.org/docs/2.4/logs.html
    // (Looks like double quoted values need not escpaing support)
    let re = Regex::new(
        r#"^([^ ]+) [^ ]+ [^ ]+ \[([^\]]+)\] "([^ "]+) ([^ "]+) ([^ "]+)" (\d+) (\d+) "([^"]+)" "([^"]+)""#,
    )
    .unwrap();

    // e.save(&mut a).unwrap();
    use itertools::Itertools;
    use rayon::prelude::ParallelIterator;
    use rayon::prelude::*;
    use ParseError::*;
    let lines = read_lines(".cache/access_log").unwrap();

    let par = lines.chunks(100000);
    let stuff = par
        .into_iter()
        // .into_iter()
        // .par_bridge()
        .map(|chunkedlines| -> Result<(), ParseError> {
            let mut c = pool.clone().get().unwrap();
            let mut lines = chunkedlines.collect::<Vec<_>>();
            let gaah = lines
                .par_drain(..)
                .map(|lineresult| -> Result<LogEntry, ParseError> {
                    let line = lineresult.unwrap();
                    if let Some(captures) = re.captures(&line) {
                        if let (
                            Some(ipmatch),
                            Some(datematch),
                            Some(methodmatch),
                            Some(urlmatch),
                            Some(protomatch),
                            Some(statusmatch),
                            Some(bytesmatch),
                            Some(referrermatch),
                            Some(useragentmatch),
                        ) = (
                            captures.get(1),
                            captures.get(2),
                            captures.get(3),
                            captures.get(4),
                            captures.get(5),
                            captures.get(6),
                            captures.get(7),
                            captures.get(8),
                            captures.get(9),
                        ) {
                            let ip =
                                IpAddr::from_str(ipmatch.as_str()).map_err(|_| ParsingError)?;
                            let dtime = chrono::DateTime::parse_from_str(
                                datematch.as_str(),
                                "%d/%b/%Y:%H:%M:%S %z",
                            )
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
                            hasher.update(ipmatch.as_str());
                            hasher.update(&useragent);
                            let hash = hasher.finalize();
                            Ok(LogEntry {
                                hash: hash.as_slice().to_owned(),
                                timestamp: dtime,
                                method: method.to_owned(),
                                url: url.to_owned(),
                                referrer: referrer.to_owned(),
                                status: status,
                                useragent: useragent.to_owned(),
                            })
                        } else {
                            // TODO: Collect errors and send in a channel to report on stderr
                            // println!("Parsing row failed {}", &l);
                            Err(ParsingError)
                        }
                    } else {
                        // println!("Parsing row failed 2 {}", &l);
                        Err(ParsingError)
                    }
                });

            let tx = c.transaction().unwrap();
            let rows = gaah.collect::<Vec<_>>();
            {
                let mut insertor = BatchInsertor::new(&tx);
                rows.iter().for_each(|r| {
                    if let Ok(row) = r {
                        insertor.add(&row);
                    }
                });
                println!("ADDED A CHUNK");
                // rows.into_iter().for_each(|r| {
                //     if let Ok(row) = r {
                //         insertor.add(&row);
                //     }
                // });
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
