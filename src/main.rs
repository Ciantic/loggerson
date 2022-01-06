use itertools::Either;
use itertools::Itertools;
use rayon::prelude::ParallelIterator;
use rayon::prelude::*;
use std::io::BufRead;
use std::thread;
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

pub enum Error {
    LogParseError(ParseError),
    SqliteError(rusqlite::Error),
    LogFileIOError(io::Error),
}

enum ChunkMsg {
    Lines(Vec<LogEntry>),
    EndOfLines,
}

fn main() {
    let conpool = init(".cache.db").unwrap();
    let (chunks_sender, chunks_receiver) = std::sync::mpsc::sync_channel::<ChunkMsg>(3);
    let (error_sender, error_receiver) = std::sync::mpsc::channel();

    // Parser thread
    thread::spawn(move || {
        let lines = read_lines(".cache/access_log").unwrap();
        let line_chunks = lines.chunks(100000);
        line_chunks
            .into_iter()
            .enumerate()
            .map(|(chunk_n, chunkedlines)| -> Result<(), ParseError> {
                let measure = Instant::now();
                let mut lines = chunkedlines.collect_vec();

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

                println!(
                    "Parsed a chunk in {} ms, successes {}, failures {}.",
                    (Instant::now() - measure).as_millis(),
                    entries.len(),
                    failures.len()
                );

                chunks_sender.send(ChunkMsg::Lines(entries)).unwrap();
                Ok(())
            })
            .for_each(drop);
        chunks_sender.send(ChunkMsg::EndOfLines).unwrap();
    });

    // SQL Insert thread
    thread::spawn(move || {
        let mut cache = BatchCache::new();

        // Pre-populate caches
        cache
            .populate(&conpool.get().unwrap(), &error_sender)
            .unwrap();

        for chunk_message in chunks_receiver {
            match chunk_message {
                ChunkMsg::Lines(entries) => {
                    let start_time = Instant::now();
                    println!("Batch insert started...");

                    let mut conn = conpool.get().unwrap();
                    let tx = conn.transaction().unwrap();
                    batch_insert(&error_sender, &tx, &entries, &mut cache).unwrap();
                    tx.commit().unwrap();

                    println!(
                        "Batch insert ended in {} ms.",
                        (Instant::now() - start_time).as_millis()
                    );
                }
                ChunkMsg::EndOfLines => break,
            }
        }
    });

    // Listen any errors inside the batch inserts
    for error in error_receiver {
        println!("Error: {:?}", error);
    }
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
