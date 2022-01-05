use itertools::Either;
use itertools::Itertools;
use rayon::prelude::ParallelIterator;
use rayon::prelude::*;
use std::io::BufRead;
use std::{fs::File, io, path::Path, time::Instant};

use crate::db::batch_insert;
use crate::db::{init, BatchCache};
use crate::models::LogEntry;
use crate::parser::parse;
use crate::parser::ParseError;

mod db;
mod iterutils;
mod models;
mod parser;

fn main() {
    let conpool = init(".cache.db").unwrap();
    let lines = read_lines(".cache/access_log").unwrap();
    let par = lines.chunks(100000);

    let (error_sender, error_receiver) = std::sync::mpsc::channel();
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
                    batch_insert(&error_sender, &tx, &entries, &mut cache).unwrap();
                }
                tx.commit().unwrap();
                Ok(())
            });

    stuff.for_each(|_r| {});

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
