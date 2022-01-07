use derive_more::{Display, Error, From};
use itertools::Itertools;
use rayon::prelude::ParallelIterator;
use rayon::prelude::*;
use std::io::BufRead;
use std::thread;
use std::{fs::File, io, path::Path, time::Instant};
use utils::SendErrorsExt;

use crate::db::batch_insert;
use crate::db::{init, BatchCache};
use crate::models::LogEntry;
use crate::parser::parse;
use crate::parser::ParseError;
use crate::utils::{ParallelMapErrsExt, ParallelSendErrorsExt};

mod db;
mod models;
mod parser;
mod utils;

#[derive(From, Debug, Error, Display)]
pub enum Error {
    LogParseError(ParseError),
    SqliteError(rusqlite::Error),
    LogFileIOError(io::Error),
}

#[derive(From, Debug)]
pub enum DiagMsg {
    Error(Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

enum ChunkMsg {
    Lines(Vec<LogEntry>),
    EndOfLines,
}

static CHUNK_SIZE: usize = 100000;
static CHUNK_QUEUE: usize = 3;

fn main() {
    let conpool = init(".cache.db").unwrap();
    let (chunks_sender, chunks_receiver) = crossbeam_channel::bounded::<ChunkMsg>(CHUNK_QUEUE);
    let (error_sender, error_receiver) = crossbeam_channel::unbounded::<DiagMsg>();

    // Parser thread
    let parser_error_sender = error_sender.clone();
    thread::spawn(move || {
        let lines = read_lines(".cache/access_log").unwrap();
        let line_chunks = lines.chunks(CHUNK_SIZE);
        line_chunks
            .into_iter()
            .enumerate()
            .for_each(|(chunk_n, chunkedlines)| -> () {
                let measure = Instant::now();
                let lines = chunkedlines.collect_vec();

                // Parse all rows in parallel
                let mut entries = lines
                    .into_par_iter()
                    .map_errs(Error::LogFileIOError)
                    .send_errors(&parser_error_sender)
                    .map(parse)
                    .map_errs(Error::LogParseError)
                    .send_errors(&parser_error_sender)
                    .collect::<Vec<_>>();

                // Sort by timestamp
                entries.par_sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

                println!(
                    "Parsed a chunk {}. of size {} in {} ms.",
                    chunk_n,
                    entries.len(),
                    (Instant::now() - measure).as_millis()
                );

                chunks_sender.send(ChunkMsg::Lines(entries)).unwrap();
            });
        chunks_sender.send(ChunkMsg::EndOfLines).unwrap();
    });

    // SQL Insert thread
    let insert_error_sender = error_sender.clone();
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
                    batch_insert(&insert_error_sender, &tx, &entries, &mut cache).unwrap();
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
