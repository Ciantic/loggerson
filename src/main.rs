use crossbeam_channel::{Receiver, Sender};
use derive_more::From;
use itertools::Itertools;
use rayon::prelude::*;
use std::io::{BufRead, BufReader, Write};
use std::thread;
use std::time::Duration;
use std::{fs::File, io, time::Instant};
use utils::{MapErrsExt, SendErrorsExt};

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

#[derive(From, Debug)]
pub enum Msg {
    LogParseError(ParseError),
    LogFileIOError(io::Error),
    DbError(db::DbError),
    RowParsed,
    RowUnique,
    RowInserted,
    AllParsingDone,
    AllInsertDone,
}

#[derive(From, Debug)]
enum ChunkMsg {
    Lines(Vec<LogEntry>),
}

#[derive(Debug)]
struct DrawState {
    parse_errors: usize,
    parsed: usize,
    unique: usize,
    insert_errors: usize,
    duplicates: usize,
    insertted: usize,
    drawed: Instant,
    started: Instant,
    ended: Option<Instant>,
    // errors: Vec<Error>,
}

impl DrawState {
    fn new() -> Self {
        Self {
            duplicates: 0,
            insert_errors: 0,
            insertted: 0,
            unique: 0,
            // last_errors: None,
            parse_errors: 0,
            parsed: 0,
            started: Instant::now(),
            drawed: Instant::now(),
            ended: None,
            // errors: Vec::new(),
        }
    }
}

static CHUNK_SIZE: usize = 100000;
static CHUNK_QUEUE: usize = 3;
static TERMINAL_MS_PER_FRAME: u128 = 30; // Approx ~33 fps (1000 / 33 = 30ms per frame)

/// This application is made of three threads, with following data flow:
///
/// * Parser -> SQL Insert
/// * Parser -> Main msg thread
/// * SQL Insert -> Main msg thread
///
/// Additionally the Parser creates worker threads with Rayon. Each thread
/// should exit gracefully.
fn main() {
    let (chunks_sender, chunks_receiver) = crossbeam_channel::bounded::<ChunkMsg>(CHUNK_QUEUE);
    let (msg_sender, msg_receiver) = crossbeam_channel::unbounded::<Msg>();

    // Parser thread
    let msg_sender_for_parser = msg_sender.clone();
    thread::spawn(move || parser_thread(msg_sender_for_parser, chunks_sender));

    // SQL Insert thread
    thread::spawn(move || sql_insert_thread(msg_sender, chunks_receiver));

    msg_thread(msg_receiver)
}

fn parser_thread(msg_sender: Sender<Msg>, chunks_sender: Sender<ChunkMsg>) {
    let file = File::open(".cache/access_log").unwrap();
    let lines = BufReader::new(file).lines();
    let line_chunks = lines.chunks(CHUNK_SIZE);

    line_chunks.into_iter().for_each(|chunkedlines| -> () {
        let lines = chunkedlines.collect_vec();

        // Parse all rows in parallel
        let mut entries = lines
            .into_par_iter()
            .map_errs(Msg::LogFileIOError)
            .send_errors(&msg_sender)
            .map(parse)
            .map_errs(Msg::LogParseError)
            .send_errors(&msg_sender)
            .map(|e| {
                msg_sender.send(Msg::RowParsed).unwrap();
                e
            })
            .collect::<Vec<_>>()
            .into_iter()
            .unique_by(|e| (e.timestamp, e.user.hash, e.request.clone()))
            .map(|e| {
                msg_sender.send(Msg::RowUnique).unwrap();
                e
            })
            .collect_vec();

        // Sort by timestamp
        entries.par_sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

        chunks_sender.send(ChunkMsg::Lines(entries)).unwrap();
    });
    msg_sender.send(Msg::AllParsingDone).unwrap();
}

fn sql_insert_thread(msg_sender: Sender<Msg>, chunks_receiver: Receiver<ChunkMsg>) {
    let conpool = init(".cache.db").unwrap();
    let mut cache = BatchCache::new();

    // Pre-populate caches
    cache
        .populate(&conpool.get().unwrap(), &msg_sender)
        .unwrap();

    for chunk_message in chunks_receiver {
        match chunk_message {
            ChunkMsg::Lines(entries) => {
                let mut conn = conpool.get().unwrap();
                let tx = conn.transaction().unwrap();
                batch_insert(&msg_sender, &tx, &entries, &mut cache).unwrap();
                tx.commit().unwrap();
            }
        }
    }
    msg_sender.send(Msg::AllInsertDone).unwrap();
}

fn msg_thread(msg_receiver: Receiver<Msg>) {
    let mut draw_state = DrawState::new();

    let mut send_draw = move || -> bool {
        let now = Instant::now();
        if (now - draw_state.drawed).as_millis() > TERMINAL_MS_PER_FRAME {
            draw_state.drawed = now;
            true
        } else {
            false
        }
    };

    loop {
        use crossbeam_channel::RecvTimeoutError::*;
        match msg_receiver.recv_timeout(Duration::from_millis(TERMINAL_MS_PER_FRAME as u64)) {
            Ok(msg) => match msg {
                Msg::RowInserted => draw_state.insertted += 1,
                Msg::RowParsed => draw_state.parsed += 1,
                Msg::RowUnique => draw_state.unique += 1,
                Msg::AllParsingDone => {}
                Msg::AllInsertDone => {}
                Msg::LogFileIOError(_) => {}
                Msg::LogParseError(_) => draw_state.parse_errors += 1,
                Msg::DbError(err) => match err {
                    db::DbError::SqliteError(_) => draw_state.insert_errors += 1,
                    db::DbError::DuplicateEntry => draw_state.duplicates += 1,
                },
            },
            Err(Disconnected) => break,
            Err(Timeout) => (),
        }

        if send_draw() {
            draw(&draw_state);
        }
    }

    draw_state.ended = Some(Instant::now());
    draw(&draw_state);
}

fn draw(state: &DrawState) {
    print!(
        "\rParsed {}, errors {}, unique ~{}. Inserted {}, duplicates {}, insert errors {}.",
        state.parsed,
        state.parse_errors,
        state.unique,
        state.insertted,
        state.duplicates,
        state.insert_errors
    );
    let _ = io::stdout().flush();
    if let Some(ended) = state.ended {
        println!("");
        println!("Done in {} ms.", (ended - state.started).as_millis());
    }
}
