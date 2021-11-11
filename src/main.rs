use bastion::spawn;
use bastion::Bastion;
use chrono::Utc;
use serde::Deserialize;
use serde::Serialize;
use sled::Db;
use smol::Timer;
use std::fs::File;
use std::io::Read;
use std::time::Duration;
use std::time::SystemTime;

use ledb::{
    query, query_extr, Comp, Document, Filter, Identifier, IndexKind, KeyType, Options, Order,
    OrderKind, Primary, Storage,
};

use std::str::FromStr;

const LIMIT_STEP: i64 = 20_000_000_000i64;
const ONE_SEC: i64 = 1_000_000_000i64;
const ONE_MIL_SEC: i64 = 1_000_000i64;
const ONE_NAN_SEC: i64 = 1i64;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Document)]
struct MyDoc {
    #[document(primary, index)]
    id: Option<Primary>,
    #[document(index)]
    timestamp: i64,
    frame: Vec<u8>,
}
fn main() {
    // let config = config::Config::from_args(std::env::args()).unwrap();

    Bastion::init();
    Bastion::start();

    insert();
    //spawn!(query(1636432243220342000, 1636493293220342000));

    // let path = format!("src/record/{}", 1);
    // let storage = Storage::new(&path, Options::default()).unwrap();

    // // Get collection
    // let collection = storage.collection("record").unwrap();

    // // Ensure indexes
    // query!(index for collection
    //     timestamp int unique,
    // )
    // .unwrap();

    // let bef = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
    //     Ok(n) => n.as_nanos(),
    //     Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    // };

    // let filter = query!(@filter timestamp in 1636616633633086000..1636616647280748000);
    // let elements: Vec<MyDoc> = collection
    //     .find(filter, Order::Primary(OrderKind::Asc))
    //     .unwrap()
    //     .collect::<Result<Vec<_>, _>>()
    //     .unwrap();

    // let aft = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
    //     Ok(n) => n.as_nanos(),
    //     Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    // };
    // println!("{}", (aft - bef) / 1_000_000u128);
    // println!("len: {}", elements.len());
    // for element in elements {
    //     println!("{}", element.timestamp);
    // }
    // println!("{:?}", found_docs.last().unwrap().id);

    Bastion::block_until_stopped();
}

fn insert() {
    let mut file = File::open("src/logo.png").unwrap();
    let mut contents = vec![];
    file.read_to_end(&mut contents).unwrap();

    for i in 1..=30 {
        let contents = contents.clone();
        let path = format!("src/record/{}", i);
        // let record_db_config = sled::Config::default()
        //     .path(format!("src/record/{}", i))
        //     .cache_capacity(10 * 1024 * 1024)
        //     .mode(sled::Mode::HighThroughput);
        //let record_db = record_db_config.open().unwrap();
        // let mut db_opts = Options::default();
        // db_opts.create_if_missing(true);
        // let record_db: DB = DB::open(&db_opts, path).unwrap();
        // Open storage
        let storage = Storage::new(&path, Options::default()).unwrap();
        unsafe { storage.set_mapsize(1024 * 1024 * 1024 * 30) };

        // Get collection
        let collection = storage.collection("record").unwrap();

        // Ensure indexes using document type
        query!(index MyDoc for collection).unwrap();

        bastion::spawn!(async move {
            let mut i: i64 = 0;
            while i < 432000 {
                let now = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
                    Ok(n) => n.as_nanos(),
                    Err(_) => panic!("SystemTime before UNIX EPOCH!"),
                };
                //if !record_db.contains_key(now.to_string().as_bytes()).unwrap() {
                // let _ = record_db.put(now.to_string().as_bytes(), contents.to_vec());
                let first_id = collection
                    .insert(&MyDoc {
                        id: None,
                        timestamp: now as i64,
                        frame: contents.to_vec(),
                    })
                    .unwrap();

                println!("{}", i);
                i += 1;

                Timer::after(Duration::from_millis(50)).await;
                //}
            }
        });
    }
}

async fn query_db(start_time: i64, end_time: i64) {
    println!("QUERYYY");
    let record_db_config = sled::Config::default()
        .path(format!("src/record/{}", 1))
        .cache_capacity(10 * 1024 * 1024)
        // .flush_every_ms(Some(200))
        .mode(sled::Mode::HighThroughput);
    let record_db = record_db_config.open().unwrap();
    let mut sleep = 0;

    let mut limit_step = 0;

    let start_record = get_start_record_time(record_db.clone());
    println!("Start record: {}", start_record);
    let end_record = get_end_record_time(record_db.clone());
    println!("End record: {}", end_record);

    if start_time <= end_record && end_time >= start_record && start_time <= end_time {
        println!("YUPPP");
        let mut start = if start_time < start_record {
            start_record
        } else {
            start_time
        };
        let end = if end_time > end_record {
            end_record
        } else {
            end_time
        };

        println!("Start time: {} and End time: {}", start, end);

        while start <= end {
            let record_db = record_db.clone();
            limit_step = if end - start >= limit_step {
                LIMIT_STEP
            } else {
                end - start + ONE_SEC
            };

            let frames = range_query(
                record_db,
                start.to_string().as_bytes(),
                (start + limit_step).to_string().as_bytes(),
                1,
            );
            println!("Vec length: {}", frames.len());

            let mut cur_index = 0;
            for i in 0..frames.len() {
                let frame = &frames[i];
                if i < frames.len() - 1 {
                    let next_frame = &frames[i + 1];
                    sleep = ((next_frame.0 - frame.0) / 1_000_000i64) as u64;
                }
                cur_index = frame.0;
                println!("{}", frame.0);

                Timer::after(Duration::from_millis(sleep)).await;
            }

            println!("Limit: {}", limit_step);

            start = cur_index + ONE_NAN_SEC;
        }
    }
}

pub fn range_query(db: Db, start: &[u8], end: &[u8], speed: usize) -> Vec<(i64, Vec<u8>)> {
    let query = db.range(start..end);
    let mut value_array: Vec<(i64, Vec<u8>)> = Vec::<(i64, Vec<u8>)>::new();
    for q in query.step_by(speed) {
        if let Ok(tuple_res) = q {
            let timestamp_str = String::from_utf8(tuple_res.0.to_vec()).unwrap();
            let timestamp = i64::from_str(&timestamp_str).unwrap();
            let data = tuple_res.1.to_vec();

            value_array.push((timestamp, data));
        } else {
            break;
        }
    }
    value_array
}

pub fn get_start_record_time(db: Db) -> i64 {
    let result = match db.first().unwrap() {
        Some(frame) => {
            let frame_key_str = String::from_utf8(frame.0.to_vec()).unwrap();
            i64::from_str(&frame_key_str).unwrap()
        }
        None => 0,
    };
    result
}

pub fn get_end_record_time(db: Db) -> i64 {
    let result = match db.last().unwrap() {
        Some(frame) => {
            let frame_key_str = String::from_utf8(frame.0.to_vec()).unwrap();
            i64::from_str(&frame_key_str).unwrap()
        }
        None => 0,
    };
    result
}
