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

use crate::db::batch_insert;
use crate::db::{init, BatchCache};
use crate::models::LogEntry;
use crate::parser::parse;
use crate::parser::ParseError;

mod db;
mod models;
mod parser;

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
                    .map(|lineresult| parse(lineresult.unwrap()));

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
                    // let mut insertor = BatchInsertor::new(&tx, &mut cache);
                    // insertor.add_entries(&entries);
                    batch_insert(&tx, &entries, &mut cache);
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
