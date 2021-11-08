mod config;
mod publisher_actor;
mod test_actor;
mod throttle;

use bastion::Bastion;
use bastion::{run, spawn};
use chrono::Utc;
use log::LevelFilter;
use log::{info, trace, warn};
use smol::Timer;
use std::fs::File;
use std::io::Read;
use std::time::Duration;
use std::time::SystemTime;

use std::{
    convert::{TryFrom, TryInto},
    io::Write,
    str::FromStr,
    sync::Arc,
};

use crate::test_actor::TestActor;

fn main() {
    // let config = config::Config::from_args(std::env::args()).unwrap();

    Bastion::init();
    Bastion::start();

    //insert();
    query();

    Bastion::block_until_stopped();
}

fn insert() {
    let mut file = File::open("src/logo.png").unwrap();
    let mut contents = vec![];
    file.read_to_end(&mut contents).unwrap();

    for i in 1..=30 {
        let contents = contents.clone();
        let record_db_config = sled::Config::default()
            .path(format!("src/record/{}", i))
            .cache_capacity(10 * 1024 * 1024)
            // .flush_every_ms(Some(200))
            .mode(sled::Mode::HighThroughput);
        let record_db = record_db_config.open().unwrap();
        spawn!(async move {
            let mut i = 0;
            while i < 20 {
                let mut batch = sled::Batch::default();
                let mut j = 0;
                while j < 10 {
                    let now = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
                        Ok(n) => n.as_nanos(),
                        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
                    };

                    let _ = batch.insert(now.to_string().as_bytes(), contents.to_vec());
                    println!("J: {}", j);

                    j += 1;

                    Timer::after(Duration::from_millis(200)).await;
                }
                println!("I: {}", j);
                i += 1;

                record_db.apply_batch(batch).unwrap();
            }
        });
    }
}

fn query() {
    let record_db_config = sled::Config::default()
        .path(format!("src/record/{}", 1))
        .cache_capacity(10 * 1024 * 1024)
        // .flush_every_ms(Some(200))
        .mode(sled::Mode::HighThroughput);
    let record_db = record_db_config.open().unwrap();

    let frame = record_db.first().unwrap().unwrap();
    let frame_key_str = String::from_utf8(frame.0.to_vec()).unwrap();
    let start = i64::from_str(&frame_key_str).unwrap();
    let frame_l = record_db.last().unwrap().unwrap();
    let frame_key_l_str = String::from_utf8(frame_l.0.to_vec()).unwrap();
    let last = i64::from_str(&frame_key_l_str).unwrap();
    println!("start: {}", start);
    println!("last: {}", last);
    println!("size: {}", (last - start) / 1_000_000_000i64);
    let end = (start / 1_000_000_000i64) + 20;

    let start_str = start.to_string();
    let start_bin = start_str.as_bytes();
    let end_str = end.to_string();
    let end_bin = end_str.as_bytes();

    let bef = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(n) => n.as_nanos(),
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    };

    let data = record_db.range(start_bin..end_bin);

    let aft = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(n) => n.as_nanos(),
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    };
    println!("Query time: {}", aft - bef);
}
