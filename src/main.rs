pub mod record_saving_actor;

use async_std::fs::create_dir;
use bastion::distributor::Distributor;
use bastion::spawn;
use bastion::supervisor::SupervisionStrategy;
use bastion::Bastion;
use chrono::Utc;
use dashmap::DashMap;
use ledb::Collection;
use m3u8_rs::Playlist;
use notify::{watcher, RecursiveMode, Watcher};
use serde::Deserialize;
use serde::Serialize;
use sled::Db;
use smol::Timer;
use std::fs;
use std::fs::create_dir_all;
use std::fs::File;
use std::io::Read;
use std::io::Write;
use std::sync::mpsc::channel;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use tokio::io::AsyncWriteExt;

use ledb::{
    query, query_extr, Comp, Document, Filter, Identifier, IndexKind, KeyType, Options, Order,
    OrderKind, Primary, Storage,
};

use std::str::FromStr;

use crate::record_saving_actor::RecordSavingActor;
use crate::record_saving_actor::SaveRecordFrameMessage;

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
}
fn main() {
    // let config = config::Config::from_args(std::env::args()).unwrap();

    Bastion::init();
    Bastion::start();

    watch_file();
    // insert();
    // pawn!(query_db(1636637808736768110, 1636957818736768110));

    // let path = format!("/data/record/{}", 1);
    // let storage = Storage::new(&path, Options::default()).unwrap();
    // unsafe { storage.set_mapsize(1024 * 1024 * 1024 * 25) };

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

fn watch_file() {
    // Create a channel to receive the events.
    let (tx, rx) = channel();

    // Create a watcher object, delivering debounced events.
    // The notification back-end is selected based on the platform.
    let mut watcher = notify::watcher(tx, Duration::from_secs(0)).unwrap();

    // Add a path to be watched. All files and directories at that path and
    // below will be monitored for changes.
    // let watcher_path = "/Users/shint1001/Desktop/hls";
    let root_path = "/home/lexhub/";
    let _ = create_dir_all(format!("{}/hls", root_path));
    let _ = create_dir_all(format!("{}/hls_cp", root_path));
    let _ = create_dir_all(format!("{}/m3u8", root_path));
    watcher
        .watch(format!("{}/hls", root_path), RecursiveMode::Recursive)
        .unwrap();
    // fs::remove_dir_all(watcher_path).unwrap();
    // fs::create_dir(watcher_path).unwrap();

    let map: Arc<DashMap<String, i64>> = Arc::new(DashMap::new());
    loop {
        match rx.recv() {
            Ok(event) => {
                let now = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
                    Ok(n) => n.as_nanos(),
                    Err(_) => panic!("SystemTime before UNIX EPOCH!"),
                };
                let map = map.clone();

                // println!("{:?} on {:?}", event, now);

                match event {
                    notify::DebouncedEvent::NoticeWrite(_) => {}
                    notify::DebouncedEvent::NoticeRemove(path) => {
                        let file_name = path.file_name().unwrap().to_str().unwrap().to_owned();
                        let is_contains = map.contains_key(&file_name);
                        if is_contains {
                            map.remove(&file_name);
                        }
                    }
                    // notify::DebouncedEvent::Write(path) => {
                    //     println!("WROTE: {:?} on {:?}", path, now);
                    // }
                    // notify::DebouncedEvent::Create(path) => {
                    //     println!("CREATE: {:?} on {:?}", path, now);
                    // }
                    notify::DebouncedEvent::Create(path) | notify::DebouncedEvent::Write(path) => {
                        let file_name = path.file_name().unwrap().to_str().unwrap().to_owned();
                        let is_contains = map.contains_key(&file_name);
                        if is_contains {
                            let time = *map.get(&file_name).unwrap();
                            println!("FILE NAME: {}", file_name);
                            let _ = fs::copy(
                                format!("{}/hls/{}", root_path, file_name),
                                format!("{}/hls_cp/{}", root_path, file_name),
                            );
                            let _ = fs::rename(
                                format!("{}/hls_cp/{}", root_path, file_name),
                                format!("{}/hls_cp/{}.ts", root_path, time),
                            );

                            let mut file =
                                std::fs::File::open(format!("{}/m3u8/hlstest.m3u8", root_path))
                                    .unwrap();
                            let mut bytes: Vec<u8> = Vec::new();
                            file.read_to_end(&mut bytes).unwrap();

                            match m3u8_rs::parse_playlist(&bytes) {
                                Result::Ok((_, Playlist::MasterPlaylist(pl))) => {
                                    println!("Master playlist:\n{:?}", pl)
                                }
                                Result::Ok((_, Playlist::MediaPlaylist(pl))) => {
                                    for media in pl.segments.clone() {
                                        if media.uri == file_name {
                                            println!(
                                                "FILE: {} duration: {}",
                                                file_name, media.duration
                                            );
                                        }
                                    }
                                    // println!("Media playlist:\n{:?}", pl)
                                }
                                Result::Err(e) => panic!("Parsing error: \n{}", e),
                            }

                            map.remove(&file_name);
                        } else {
                            if file_name.contains(".ts") {
                                map.insert(file_name, now as i64);
                            }
                        }

                        // println!("MAP: {:?}", map);
                    }
                    notify::DebouncedEvent::Chmod(_) => {}
                    notify::DebouncedEvent::Remove(_) => {}
                    notify::DebouncedEvent::Rename(_, _) => {}
                    notify::DebouncedEvent::Rescan => {}
                    notify::DebouncedEvent::Error(_, _) => {}
                }
            }
            Err(e) => println!("watch error: {:?}", e),
        }
    }
}

fn insert() {
    let mut file = File::open("src/logo.png").unwrap();
    let mut contents = vec![];
    file.read_to_end(&mut contents).unwrap();

    let parent_ref = Bastion::supervisor(|sp| sp.with_strategy(SupervisionStrategy::OneForOne))
        .expect("could not create a supervisor");

    let mut count = 0;

    let start = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(n) => n.as_nanos(),
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    };

    // for i in 0..39 {
    for j in 0..256000 {
        // for j in 0..256 {
        println!("COUNT: {}", count);
        // let now = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        //     Ok(n) => n.as_nanos(),
        //     Err(_) => panic!("SystemTime before UNIX EPOCH!"),
        // };

        let folder_url = format!("/data/record_frames/{}/{}", "2022-05-17", "39",);

        match fs::create_dir_all(&folder_url) {
            Ok(_) => {
                let file_url = format!("{}/{}", folder_url, j);

                let mut file = File::create(file_url.clone()).unwrap();
                file.write_all(&contents).unwrap();
            }
            Err(_) => {}
        };
        count += 1;
    }

    // }
    // }

    let end = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(n) => n.as_nanos(),
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    };

    println!("SAVE SUCCESSFUL: {}", end - start);

    // let mut count = 0;

    // let start = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
    //     Ok(n) => n.as_nanos(),
    //     Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    // };

    // for i in 0..39 {
    //     let folder_url = format!("/data/record_frames/{}/{}", "2022-05-17", i,);
    //     let _ = std::fs::remove_dir_all(folder_url);
    //     count += 1;
    // }

    // let end = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
    //     Ok(n) => n.as_nanos(),
    //     Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    // };

    // println!("DELETE SUCCESSFUL: {}", end - start);

    // let total_size = count * (23 * 256000) / 1024;
    // let total_time = end - start;

    // println!("DELETE SUCCESSFUL total_size :  {} Mb", total_size);
    // println!("DELETE SUCCESSFUL total_size :  {} Gb", total_size / 1024);
    // println!("DELETE SUCCESSFUL total_time:   {} s", total_time);
    // println!("DELETE SUCCESSFUL:   {} Mb/s", total_size / total_time);

    // for i in 1..=39 {
    //     let contents = contents.clone();
    //     let parent_ref = parent_ref.clone();
    //     spawn!(async move {
    //         let (_, mut record_saving_rx) =
    //             RecordSavingActor::init(&parent_ref, i.to_string()).unwrap();

    //         let rt = tokio::runtime::Runtime::new().unwrap();
    //         rt.block_on(async {
    //             let _ = record_saving_rx.recv().await;
    //         });

    //         spawn!(async move {
    //             let mut j: i64 = 0;
    //             while j < 432000 {
    //                 let contents = contents.clone();
    //                 let now = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
    //                     Ok(n) => n.as_nanos(),
    //                     Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    //                 };

    //                 let msg_to_record_saving = SaveRecordFrameMessage {
    //                     cam_id: i.to_string(),
    //                     timestamp: now as i64,
    //                     payload: contents,
    //                     record_cloud: false,
    //                 };

    //                 let record_saving_actor =
    //                     Distributor::named(format!("record_saving_actor_{}", i.to_string()));
    //                 record_saving_actor
    //                     .tell_one(msg_to_record_saving)
    //                     .expect("Can't send the message!");

    //                 println!("CAM: {}, SEQ: {}", i, j);
    //                 j += 1;

    //                 Timer::after(Duration::from_millis(333)).await;
    //             }
    //             // }
    //         });
    //     });
    // }
}

// async fn query_db(start_time: i64, end_time: i64) {
//     println!("QUERYYY");
//     let path = format!("src/record/{}", 1);
//     let storage = Storage::new(&path, Options::default()).unwrap();

//     // Get collection
//     let collection = storage.collection("record").unwrap();

//     // Ensure indexes
//     query!(index for collection
//         timestamp int unique,
//     )
//     .unwrap();

//     let mut sleep = 0;

//     let mut limit_step = 0;

//     let start_record = get_start_record_time(collection.clone());
//     println!("Start record: {}", start_record);
//     let end_record = get_end_record_time(collection.clone());
//     println!("End record: {}", end_record);

//     if start_time <= end_record && end_time >= start_record && start_time <= end_time {
//         println!("YUPPP");
//         let mut start = if start_time < start_record {
//             start_record
//         } else {
//             start_time
//         };
//         let end = if end_time > end_record {
//             end_record
//         } else {
//             end_time
//         };

//         println!("Start time: {} and End time: {}", start, end);

//         while start <= end {
//             let collection = collection.clone();
//             limit_step = if end - start >= limit_step {
//                 LIMIT_STEP
//             } else {
//                 end - start + ONE_SEC
//             };

//             let frames = range_query(collection, start, start + limit_step, 1);
//             println!("Vec length: {}", frames.len());

//             let mut cur_index = 0;
//             for i in 0..frames.len() {
//                 let frame = &frames[i];
//                 if i < frames.len() - 1 {
//                     let next_frame = &frames[i + 1];
//                     sleep = ((next_frame.0 - frame.0) / 1_000_000i64) as u64;
//                 }
//                 cur_index = frame.0;
//                 println!("{}", frame.0);

//                 Timer::after(Duration::from_millis(sleep)).await;
//             }

//             println!("Limit: {}", limit_step);

//             start = cur_index + ONE_NAN_SEC;
//         }
//     }
// }

// pub fn range_query(collection: Collection, start: i64, end: i64, speed: usize) -> Vec<i64> {
//     let filter = query!(@filter timestamp in start..end);
//     let elements: Vec<MyDoc> = collection
//         .find(filter, Order::Primary(OrderKind::Asc))
//         .unwrap()
//         .collect::<Result<Vec<_>, _>>()
//         .unwrap();
//     let mut value_array: Vec<(i64, Vec<u8>)> = Vec::<i64>::new();
//     for q in elements.into_iter().step_by(speed) {
//         value_array.push((q.timestamp));
//     }
//     value_array
// }

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
