use bastion::spawn;
use bastion::Bastion;
use chrono::Utc;
use ledb::Collection;
use serde::Deserialize;
use serde::Serialize;
use sled::Db;
use smol::Timer;
use std::fs;
use std::fs::File;
use std::io::Read;
use std::io::Write;
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
    // pawn!(query_db(1636637808736768110, 1636957818736768110));

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

    // let filter = query!(@filter timestamp in 1636637808736768110..1636657818736768110);
    // let elements: Vec<MyDoc> = collection
    //     .find(filter, Order::Primary(OrderKind::Asc))
    //     .unwrap()
    //     .collect::<Result<Vec<_>, _>>()
    //     .unwrap();

    // let last_id = collection.last_id().unwrap();
    // println!("last id: {}", last_id);

    // let last_frame: MyDoc = collection.get(last_id).unwrap().unwrap();
    // let url = last_frame.frame;

    // let mut f = File::open(&url).expect("no file found");
    // let metadata = fs::metadata(&url).expect("unable to read metadata");
    // let mut buffer = vec![0; metadata.len() as usize];
    // f.read(&mut buffer).expect("buffer overflow");

    // println!("last timestamp: {:?}", buffer);

    // let aft = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
    //     Ok(n) => n.as_nanos(),
    //     Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    // };
    // println!("{}", (aft - bef) / 1_000_000u128);
    // println!("len: {}", elements.len());

    Bastion::block_until_stopped();
}

fn insert() {
    let mut file = File::open("src/logo.png").unwrap();
    let mut contents = vec![];
    file.read_to_end(&mut contents).unwrap();

    for i in 1..=30 {
        let contents = contents.clone();
        let path = format!("/data/record/{}", i);
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
        unsafe { storage.set_mapsize(1024 * 1024 * 1024 * 25) };

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

                // let folder_url = format!("src/record_frame/{}/{}", "2021/11/12", i);
                // fs::create_dir_all(&folder_url).unwrap();

                // let file_url = format!("src/record_frame/{}/{}/{}", "2021/11/12", i, now);

                // let mut file = File::create(file_url.clone()).unwrap();
                // file.write_all(&contents).unwrap();

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
    let path = format!("src/record/{}", 1);
    let storage = Storage::new(&path, Options::default()).unwrap();

    // Get collection
    let collection = storage.collection("record").unwrap();

    // Ensure indexes
    query!(index for collection
        timestamp int unique,
    )
    .unwrap();

    let mut sleep = 0;

    let mut limit_step = 0;

    let start_record = get_start_record_time(collection.clone());
    println!("Start record: {}", start_record);
    let end_record = get_end_record_time(collection.clone());
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
            let collection = collection.clone();
            limit_step = if end - start >= limit_step {
                LIMIT_STEP
            } else {
                end - start + ONE_SEC
            };

            let frames = range_query(collection, start, start + limit_step, 1);
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

pub fn range_query(
    collection: Collection,
    start: i64,
    end: i64,
    speed: usize,
) -> Vec<(i64, Vec<u8>)> {
    let filter = query!(@filter timestamp in start..end);
    let elements: Vec<MyDoc> = collection
        .find(filter, Order::Primary(OrderKind::Asc))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let mut value_array: Vec<(i64, Vec<u8>)> = Vec::<(i64, Vec<u8>)>::new();
    for q in elements.into_iter().step_by(speed) {
        value_array.push((q.timestamp, q.frame));
    }
    value_array
}

pub fn get_start_record_time(collection: Collection) -> i64 {
    let first_id = 1;

    let first_frame: MyDoc = collection.get(first_id).unwrap().unwrap();
    first_frame.timestamp
}

pub fn get_end_record_time(collection: Collection) -> i64 {
    let last_id = collection.last_id().unwrap();

    let last_frame: MyDoc = collection.get(last_id).unwrap().unwrap();
    last_frame.timestamp
}
