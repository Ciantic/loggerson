use crossbeam_channel::{Receiver, Sender};
use derive_more::{Display, Error, From};
use itertools::Itertools;
use rayon::prelude::ParallelIterator;
use rayon::prelude::*;
use std::io::{BufRead, Write};
use std::thread;
use std::{fs::File, io, path::Path, time::Instant};

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

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(From, Debug)]
pub enum DiagMsg {
    Error(Error),
    RowParsed,
    RowInserted,
}

#[derive(From, Debug)]
enum ChunkMsg {
    Lines(Vec<LogEntry>),
}

#[derive(Debug, Clone)]
struct DrawState {
    parse_errors: usize,
    parsed: usize,
    insert_errors: usize,
    insertted: usize,
    started: Instant, // last_errors: Option<[String; 3]>,
    ended: Option<Instant>,
}

static CHUNK_SIZE: usize = 100000;
static CHUNK_QUEUE: usize = 3;

fn parser_thread(diag_sender: Sender<DiagMsg>, chunks_sender: Sender<ChunkMsg>) {
    let lines = read_lines(".cache/access_log").unwrap();
    let line_chunks = lines.chunks(CHUNK_SIZE);
    line_chunks
        .into_iter()
        // .enumerate()
        .for_each(|chunkedlines| -> () {
            let lines = chunkedlines.collect_vec();

            // Parse all rows in parallel
            let mut entries = lines
                .into_par_iter()
                .map_errs(Error::LogFileIOError)
                .send_errors(&diag_sender)
                .map(parse)
                .map_errs(Error::LogParseError)
                .send_errors(&diag_sender)
                .map(|e| {
                    diag_sender.send(DiagMsg::RowParsed).unwrap();
                    e
                })
                .collect::<Vec<_>>();

            // Sort by timestamp
            entries.par_sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

            chunks_sender.send(ChunkMsg::Lines(entries)).unwrap();
        });
}

fn sql_insert_thread(diag_sender: Sender<DiagMsg>, chunks_receiver: Receiver<ChunkMsg>) {
    let conpool = init(".cache.db").unwrap();
    let mut cache = BatchCache::new();

    // Pre-populate caches
    cache
        .populate(&conpool.get().unwrap(), &diag_sender)
        .unwrap();

    for chunk_message in chunks_receiver {
        match chunk_message {
            ChunkMsg::Lines(entries) => {
                let start_time = Instant::now();

                let mut conn = conpool.get().unwrap();
                let tx = conn.transaction().unwrap();
                batch_insert(&diag_sender, &tx, &entries, &mut cache).unwrap();
                tx.commit().unwrap();
            }
        }
    }
}

fn draw_thread(draw_receiver: Receiver<DrawState>) {
    for state in draw_receiver {
        print!(
            "\rParsed {}, parse errors: {}. Inserted {}, insert errors {}.",
            state.parsed, state.parse_errors, state.insertted, state.insert_errors
        );
        let _ = io::stdout().flush();
        if let Some(ended) = state.ended {
            println!("");
            println!("Done in {} ms.", (ended - state.started).as_millis());
        }
    }
}

fn diag_thread(started: Instant, diag_receiver: Receiver<DiagMsg>, draw_sender: Sender<DrawState>) {
    let mut last_draw = Instant::now();
    let mut draw_state = DrawState {
        insert_errors: 0,
        insertted: 0,
        // last_errors: None,
        parse_errors: 0,
        parsed: 0,
        started,
        ended: None,
    };

    let mut send_draw = move || -> bool {
        let now = Instant::now();
        if (now - last_draw).as_millis() > 100 {
            last_draw = now;
            true
        } else {
            false
        }
    };

    for msg in diag_receiver {
        match msg {
            DiagMsg::RowInserted => draw_state.insertted += 1,
            DiagMsg::RowParsed => draw_state.parsed += 1,
            DiagMsg::Error(err) => match err {
                Error::LogFileIOError(_) => {}
                Error::LogParseError(_) => draw_state.parse_errors += 1,
                Error::SqliteError(_) => draw_state.insert_errors += 1,
            },
        }
        if send_draw() {
            draw_sender.send(draw_state.clone()).unwrap();
        }
    }
    draw_state.ended = Some(Instant::now());
    draw_sender.send(draw_state).unwrap();
}

fn main() {
    let started = Instant::now();
    let (chunks_sender, chunks_receiver) = crossbeam_channel::bounded::<ChunkMsg>(CHUNK_QUEUE);
    let (diag_sender, diag_receiver) = crossbeam_channel::unbounded::<DiagMsg>();
    let (draw_sender, draw_receiver) = crossbeam_channel::unbounded::<DrawState>();

    // Parser thread
    let diag_sender_for_parser = diag_sender.clone();
    thread::spawn(move || parser_thread(diag_sender_for_parser, chunks_sender));

    // SQL Insert thread, move the diag sender as this is last thread
    thread::spawn(move || sql_insert_thread(diag_sender, chunks_receiver));

    // Drawing thread
    thread::spawn(move || draw_thread(draw_receiver));

    // Main diagnostic thread
    diag_thread(started, diag_receiver, draw_sender);
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
