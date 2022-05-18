pub mod record_saving_actor;

use bastion::distributor::Distributor;
use bastion::spawn;
use bastion::supervisor::SupervisionStrategy;
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

    insert();
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

fn insert() {
    let mut file = File::open("src/logo.png").unwrap();
    let mut contents = vec![];
    file.read_to_end(&mut contents).unwrap();

    let parent_ref = Bastion::supervisor(|sp| sp.with_strategy(SupervisionStrategy::OneForOne))
        .expect("could not create a supervisor");

    // let mut count = 0;

    // let start = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
    //     Ok(n) => n.as_nanos(),
    //     Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    // };

    // for i in 0..256000 {
    //     println!("COUNT: {}", count);
    //     // let now = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
    //     //     Ok(n) => n.as_nanos(),
    //     //     Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    //     // };

    //     let folder_url = format!("/data/record_frames/{}/{}", "2022-05-17", "1",);

    //     match fs::create_dir_all(&folder_url) {
    //         Ok(_) => {
    //             let file_url = format!("/data/record_frames/{}/{}/{}", "2022-05-17", "1", count);

    //             let mut file = File::create(file_url.clone()).unwrap();
    //             file.write_all(&contents).unwrap();
    //         }
    //         Err(_) => {}
    //     };

    //     count += 1;
    // }

    // let end = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
    //     Ok(n) => n.as_nanos(),
    //     Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    // };

    // println!("SAVE SUCCESSFUL: {}", end - start);

    let folder_url = format!("/data/record_frames/{}/{}", "2022-05-17", "1",);

    let start = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(n) => n.as_nanos(),
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    };

    let mut count = 0;

    let _ = std::fs::remove_dir_all(format!("{}", folder_url));

    // while count < 69959 {
    //     let mut c = 0;
    //     let start_c = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
    //         Ok(n) => n.as_nanos(),
    //         Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    //     };
    //     for i in count..count + 500 {
    //         let _ = std::fs::remove_file(format!("{}/{}", folder_url, i));
    //         c = i;
    //     }

    //     let end_c = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
    //         Ok(n) => n.as_nanos(),
    //         Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    //     };

    //     println!("DELETE 500 FILES SUCCESSFUL: {}", end_c - start_c);
    //     count = c;
    //     println!("COUNT: {}", count);
    // }

    let end = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(n) => n.as_nanos(),
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    };

    println!("DELETE SUCCESSFUL: {}", end - start);

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
