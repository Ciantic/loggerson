use std::{fs::File, io, net::IpAddr, path::Path, str::FromStr};

use chrono::{DateTime, FixedOffset};
use regex::Regex;
use std::io::BufRead;

use crate::db::{add, init};

mod db;

#[derive(Debug)]
enum ParseError {
    IoError(std::io::Error),
    ParsingError,
}

pub struct LogEntry {
    pub ip: IpAddr,
    pub timestamp: DateTime<FixedOffset>,
    pub method: String,
    pub url: String,
    pub status: i32,
    pub userAgent: String,
    pub referrer: String,
}

fn main() {
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

    let par = lines.chunks(10000);
    let stuff = par
        .into_iter()
        // .into_iter()
        // .par_bridge()
        .map(|r| -> Result<(), ParseError> {
            let mut c = pool.clone().get().unwrap();
            let tx = c.transaction().unwrap();
            let gaah = r.map(|r| -> Result<(), ParseError> {
                let l = r.unwrap(); //r.map_err(|_e| ParseError::ParsingError)?;
                if let Some(captures) = re.captures(&l) {
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
                        let ip = IpAddr::from_str(ipmatch.as_str()).map_err(|_| ParsingError)?;
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
                        let entry = LogEntry {
                            ip: ip,
                            timestamp: dtime,
                            method: method.to_owned(),
                            url: url.to_owned(),
                            referrer: referrer.to_owned(),
                            status: status,
                            userAgent: useragent.to_owned(),
                        };

                        add(&tx, &entry);
                    } else {
                        // TODO: Collect errors and send in a channel to report on stderr
                        // println!("Foo");
                    }
                } else {
                    // println!("Foo");
                }

                Ok(())
            });
            gaah.for_each(|r| {
                // println!("{:?}", r);
            });
            tx.commit().unwrap();

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
