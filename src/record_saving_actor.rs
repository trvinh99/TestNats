use std::{io::Write, sync::Arc};

use dashmap::DashMap;
use ledb::{query, Collection, Document, Options, Primary, Storage};

use bastion::prelude::*;
use chrono::{NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use tokio::{
    fs::{self, File},
    io::AsyncWriteExt,
    sync::mpsc::{channel, Receiver},
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Document)]
pub struct RecordDocument {
    #[document(primary)]
    pub id: Option<Primary>,
    #[document(index)]
    pub timestamp: i64,
    #[document(index)]
    pub record_cloud: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Document)]
pub struct RangeTimeDocument {
    #[document(primary)]
    id: Option<Primary>,
    #[document(index)]
    start: i64,
    #[document(index)]
    end: i64,
    #[document(index)]
    record_cloud: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SaveRecordFrameMessage {
    pub cam_id: String,
    pub timestamp: i64,
    pub record_cloud: bool,
    pub payload: Vec<u8>,
}

lazy_static::lazy_static! {
    pub static ref RECORD_DATABASE_MAP: Arc<DashMap<String, Collection>> = Arc::new(DashMap::new());
    pub static ref START_TIME_DATABASE_MAP: Arc<DashMap<String, Collection>> =
        Arc::new(DashMap::new());
}

pub struct RecordSavingActor {
    supervisor_ref: SupervisorRef,
    children_ref: ChildrenRef,
}

impl RecordSavingActor {
    pub fn init(parent_ref: &SupervisorRef, cam_id: String) -> Result<(Self, Receiver<i32>), ()> {
        let (tx, rx) = channel(1);
        let callbacks = Callbacks::new()
            .with_after_start(move || {
                // This will get called before others `before_start`s of this example
                // because the others all are defined for elements supervised by
                // this supervisor...
                let rt = tokio::runtime::Runtime::new().unwrap();
                let tx_1 = tx.clone();
                rt.block_on(async move {
                    println!("[RECORD SAVING] init BEFORE send tx");
                    let _ = tx_1.send(1).await;
                    println!("[RECORD SAVING] init AFTTER send tx");
                });
                println!("(sp      ) before_start");
            })
            .with_after_stop(|| {
                // This will get called after others `after_stop`s of this example
                // because the others all are defined for elements supervised by
                // this supervisor...
                println!("(sp      ) after_stop");
            });

        let supervisor_ref = parent_ref
            .supervisor(|sp| sp.with_strategy(SupervisionStrategy::OneForOne))
            .map_err(|_| {})?;

        let children_ref = supervisor_ref
            .children(move |children| {
                children
                    .with_redundancy(1)
                    .with_distributor(Distributor::named(format!(
                        "record_saving_actor_{}",
                        cam_id
                    )))
                    .with_callbacks(callbacks)
                    .with_exec(move |ctx: BastionContext| {
                        async move {
                            loop {
                                MessageHandler::new(ctx.recv().await?).on_tell(
                                    |message: SaveRecordFrameMessage, _sender| {
                                        // let message_2 = message.clone();
                                        let date = "2022-04-23".to_owned();
                                        let record_db =
                                            get_record_db(date.clone(), message.cam_id.clone());

                                        //Insert timestamp of a frame into the database.
                                        record_db
                                            .insert(&RecordDocument {
                                                id: None,
                                                timestamp: message.timestamp,
                                                record_cloud: message.record_cloud,
                                            })
                                            .unwrap();

                                        // spawn!(async move {
                                        //Save a frame data into disk.
                                        let rt = tokio::runtime::Runtime::new().unwrap();
                                        rt.spawn_blocking(|| async move {
                                            let folder_url = format!(
                                                "src/record_frame/{}/{}",
                                                date, message.cam_id,
                                            );

                                            match fs::create_dir_all(&folder_url).await {
                                                Ok(_) => {
                                                    let file_url = format!(
                                                        "src/record_frame/{}/{}/{}",
                                                        date,
                                                        message.cam_id,
                                                        message.timestamp.to_string()
                                                    );

                                                    let mut file = File::create(file_url.clone())
                                                        .await
                                                        .unwrap();
                                                    file.write_all(&message.payload).await.unwrap();
                                                }
                                                Err(_) => {}
                                            };
                                        });
                                    },
                                );
                            }
                        }
                    })
            })
            .map_err(|_| {})
            .unwrap();

        Ok((
            Self {
                supervisor_ref,
                children_ref,
            },
            rx,
        ))
    }

    pub async fn stop(&mut self) -> Result<(), ()> {
        self.supervisor_ref.stop()
    }
}

fn get_db(
    database_map: Arc<DashMap<String, Collection>>,
    cam_id: String,
    url: String,
    date: Option<String>,
    collection_name: &str,
) -> Collection {
    let map_key = match collection_name {
        "record" => {
            format!("{}-{}", cam_id, date.unwrap())
        }
        _ => {
            format!("{}", cam_id)
        }
    };
    let database_has_key = database_map.contains_key(&map_key);
    if database_has_key == false {
        println!("URL: {}", url);

        let storage = Storage::new(&url, Options::default()).unwrap();

        let collection = storage.collection(collection_name).unwrap();

        match collection_name {
            "record" => {
                query!(index RecordDocument for collection).unwrap();
            }
            _ => {
                query!(index RangeTimeDocument for collection).unwrap();
            }
        }

        database_map.insert(map_key, collection.clone());

        collection
    } else {
        let old_db = &*database_map.get(&map_key).unwrap();
        let db = old_db.clone();
        db
    }
}

pub fn get_record_db(date: String, cam_id: String) -> Collection {
    let record_db_url = format!("src/record/url/{}/{}", date, cam_id);

    let record_db = get_db(
        RECORD_DATABASE_MAP.clone(),
        cam_id.clone(),
        record_db_url,
        Some(date),
        "record",
    );
    record_db
}
