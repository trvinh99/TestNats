use protobuf::Message;

use bastion::distributor::*;
use bastion::prelude::*;
use chrono::Utc;
use std::time::Instant;

//use openjphffi;

use serde::{Serialize,Deserialize};
use dashmap::DashMap;
use std::sync::Arc;

use crate::aes;
use crate::jp2k::encode::lexray_jp2k;
use crate::protos::hub::HubFrame;
use crate::publisher_actor::EngineRequestPublish;

const IV: [u8; 16] = [
    99, 10, 152, 128, 166, 26, 133, 191, 249, 38, 14, 167, 122, 46, 4, 129,
];

const PORTS: [&str; 4] = ["8080","8081","8082","8083"];

const LIMIT_SIZE: usize = 1048576;

#[derive(Debug, Clone, Copy)]
pub enum FrameType {
    JPEG,
    J2K,
    J2C,
}

impl Default for FrameType {
    fn default() -> Self {
        Self::J2C
    }
}

impl FrameType {
    pub fn from(str: &str) -> Self {
        match str.to_lowercase().as_str() {
            "jpeg" => Self::JPEG,
            "j2k" => Self::J2K,
            "j2c" => Self::J2C,
            _ => Self::default(),
        }
    }
}

#[derive(Debug, Serialize,Deserialize)]
struct Frame {
    frame: Vec<u8>,
}

#[derive(Debug, Deserialize,Serialize)]
struct J2CFrame {
    frame: Vec<u8>,
}

struct EncodedFrameWrapper {
    frame: HubFrame,
    sequence: String,
}

#[derive(Debug, Clone)]
pub struct TranscodeEngine {
    pub supervisor_ref: SupervisorRef,
    pub children_ref: ChildrenRef,
}

fn loadbalance(host: &str) -> String {
    let index = fastrand::usize(..PORTS.len());

    let port = PORTS[index];     

    let ip = format!{"http://{}:{}/encodej2c", host, port};
    ip
}

fn loadbalance_publisher(source: Arc<DashMap<String,String>>,cam_id: String, number_publish_worker: usize) -> String {
    if source.contains_key(&cam_id) {
        return  (*source.get(&cam_id).unwrap()).clone();
 
     }
     let index = fastrand::usize(0..number_publish_worker);
 
 
     let name = format!{"publish_actor_{}",index};
     source.insert(cam_id, name.clone());
     name
}

impl TranscodeEngine {
    pub fn init(
        parent_ref: &SupervisorRef,
        redundancy: usize,
        name: &str,
        number_publish_worker: usize,
        j2c_host: String,
    ) -> Result<Self, ()> {
        let supervisor_ref = parent_ref
            .supervisor(|sp| sp.with_strategy(SupervisionStrategy::OneForOne))
            .map_err(|_| {
                println!("could not create discovery supervisor");
            })?;
        let camera_source_map : Arc<DashMap<String,String>> = Arc::new(DashMap::new());

        let children_ref = supervisor_ref
            .children(move |children| {
                children
                    .with_distributor(Distributor::named(name))
                    .with_redundancy(redundancy)
                    .with_exec(move |ctx: BastionContext| {
                        let j2c_host = j2c_host.clone();
                        let  camera_source_map_child = camera_source_map.clone();
                        let mut j2c_frame_count: i64 = 0;

                        async move {
                            loop {
                                MessageHandler::new(ctx.recv().await?)
                                    .on_tell(|frame: lexray_jp2k::FrameItem, _| unsafe {
                                        let camera_id = String::from_utf8(frame.camera_id.clone())
                                            .expect("Found invalid UTF-8");
                                        let topic = format!("live.{}.frames", camera_id);
                                        let timestamp_topic = format!("live.{}.timestamp", camera_id);
                                        let   camera_source_map_child = camera_source_map_child.clone();

                                        match frame.frame_type {
                                            FrameType::J2K => {
                                                //Encode frame time
                                                let start_encode = Instant::now();
                                                let result = lexray_jp2k::encode(
                                                    frame,
                                                );
                                                let encode_duration = start_encode.elapsed();
                                                println!(
                                                    "[TRANSCODE] ENCODE J2K FRAME DURATION of CAMERA {:?} : {:?}", camera_id, encode_duration
                                                );
                                                match result {
                                                    Option::Some(frame_result) => {
                                                        // let msg = BufferFrame {
                                                        //     camera_id: camera_id.clone(),
                                                        //     encoded_frame: frame_result.clone(),
                                                        // };
                                                        // let buffer_actor =
                                                        //     Distributor::named("buffer_actor");
                                                        // buffer_actor.tell_one(msg).expect(
                                                        //     "Send frame to Buffer Actor Failed.",
                                                        // );

                                                        // let serialized =
                                                        //         serde_json::to_string(&frame_result).unwrap();
                                                        //     log_debug!(&mut logger, "XXXXXXXXXXXX Encoded frame length (from {}): {}",
                                                        //     topic,
                                                        //     frame_result.image.len());
                                                        //     let msg = EngineRequestPublish {
                                                        //         topic,
                                                        //         payload: serialized.as_bytes().to_vec(),
                                                        //     };
                                                        //     let publish_actor =
                                                        //         Distributor::named("publisher_actor");
                                                        //     publish_actor.tell_one(msg).expect(
                                                        //         "Publish frame to Publish Actor Failed.",
                                                        //     );
                                                    }
                                                    Option::None => {
                                                        println!(
                                                            "[TRANSCODE] Skip Frame J2K NIL"
                                                        )
                                                    }
                                                }
                                            },
                                            FrameType::J2C => {
                                                let cam_id = String::from_utf8(frame.camera_id.clone()).unwrap_or_default();

                                                println!("[TRANSCODE]: recv from {} - {}", cam_id, frame.timestamp);

                                                let ip = loadbalance(&j2c_host);
                                               
                                                let send_frame = Frame {frame: frame.image};
                                                let serialized = serde_json::to_string(&send_frame).unwrap();

                                                let start_encode = Instant::now();
                                                let resp = run!(async {
                                                    surf::post(&ip)
                                                    .body(serialized)
                                                    .recv_string()
                                                    .await
                                                });
                                                let encode_duration = start_encode.elapsed();
                                                println!("[TRANSCODE-{}]: timestamp: {} - time trans: {:?}", cam_id, frame.timestamp, encode_duration);

                                                match resp {
                                                    Ok(v) => {
                                                        let resp_frame: Result<J2CFrame, serde_json::Error> = serde_json::from_str(v.as_str());

                                                        if resp_frame.is_ok() {
                                                            let resp_frame = resp_frame.unwrap();
                                                            if cam_id == "" {
                                                                println!("CAM ID IS EMPTY ");
                                                            }
                                                            // if is_record && cam_id != "" {
                                                            //     let msg_to_record_saving = RecordSavingMessage {
                                                            //         cam_id: cam_id.clone(),
                                                            //         timestamp: frame.timestamp as i64,
                                                            //         payload: resp_frame.frame.clone(),
                                                            //     };

                                                            //     let record_saving_actor = Distributor::named("record_saving_actor");
                                                            //     record_saving_actor.tell_one(msg_to_record_saving).expect("Can't send the message!")
                                                            // }
                                                            
                                                                let encrypted_frame = aes::encrypt(&resp_frame.frame, &frame.session_key, &IV);

                                                                let frame_result = match encrypted_frame {
                                                                    Ok(encoded_frame) => {
                                                                        // Option::Some(EncodedFrame {
                                                                        //     image: encoded_frame,
                                                                        //     timestamp: frame.timestamp,
                                                                        //     frame_type: "j2c".to_string(),
                                                                        //     camera_id: String::from_utf8(frame.camera_id.clone()).unwrap_or_default(),
                                                                        // })
                                                                        let mut hub_frame = HubFrame::new();
                                                                        hub_frame.set_sequence(j2c_frame_count);
                                                                        hub_frame.set_image(encoded_frame);
                                                                        hub_frame.set_timestamp(frame.timestamp as i64);
                                                                        hub_frame.set_frameType("j2c".to_owned());
                                                                        hub_frame.set_camId(cam_id.clone());
                                                                        Option::Some(hub_frame)
                                                                    },
                                                                    Err(_) => Option::None,
                                                                };

                                                                if let Some(frame) = frame_result {
                                                                    let serialized = frame.write_to_bytes().unwrap();
                                                                    if serialized.len() < LIMIT_SIZE {

                                                                        let msg = EngineRequestPublish {
                                                                            topic,
                                                                            payload: serialized,
                                                                        };
                                                                        
                                                                        let publish_actor_name = loadbalance_publisher(camera_source_map_child, cam_id.clone(), number_publish_worker);
                                                                        let publish_actor =    Distributor::named(publish_actor_name);
                                                                        
                                                                        j2c_frame_count += 1;

                                                                        publish_actor.tell_one(msg).expect(
                                                                            "Publish frame to Publish Actor Failed.",
                                                                        );

                                                                    } else {
                                                                        println!("[TRANSCODE-{}]: Over 1 MB payload: {}", cam_id, (serialized.len()) / (1024 *1024));
                                                                    }
                                                                }
                                                                // log_info!(&mut logger, "[TRANSCODE-{}]: Send frame to NATS", cam_id);
                                                                // println!("[TRANSCODE-{}]: Send frame to NATS", cam_id);

                                                        } else {
                                                            println!(
                                                                "Response from transcode server error at {}, value is err: {:?}",
                                                                ip, v
                                                            );
                                                        }
                                                    },
                                                    Err(e) => {
                                                        println!(
                                                            "Response from transcode server error at {}, resp is err: {:?}",
                                                            ip, e
                                                        );
                                                    }
                                                }

                                                // let start_encode = Instant::now();
                                                // let result = lexray_jp2k::j2c_encode(
                                                //     frame.clone(),
                                                //     logger.clone(),
                                                // );
                                                // let encode_duration = start_encode.elapsed();
                                                // log_info!(
                                                //     &mut logger,
                                                //     "[TRANSCODE] ENCODE J2C FRAME DURATION of CAMERA {:?} : {:?}", camera_id.clone(), encode_duration
                                                // );
                                                // match result {
                                                //     Option::Some(frame_result) => {
                                                //         // let msg = BufferFrame {
                                                //         //     camera_id: camera_id.clone(),
                                                //         //     encoded_frame: frame_result.clone(),
                                                //         // };
                                                //         // let buffer_actor =
                                                //         //     Distributor::named("buffer_actor");
                                                //         // buffer_actor.tell_one(msg).expect(
                                                //         //     "Send frame to Buffer Actor Failed.",
                                                //         // );

                                                //         let serialized =
                                                //                 serde_json::to_string(&frame_result).unwrap();
                                                //             log_info!(&mut logger, "[TRANSCODE] Encoded J2c frame length (to topic {}): {}",
                                                //             topic,
                                                //             frame_result.image.len());
                                                //             let msg = CommandRequestPublish {
                                                //                 topic,
                                                //                 payload: serialized.as_bytes().to_vec(),
                                                //             };
                                                //             let publish_actor =
                                                //                 Distributor::named("publisher_actor");
                                                //             publish_actor.tell_one(msg).expect(
                                                //                 "Publish frame to Publish Actor Failed.",
                                                //             );
                                                //     }
                                                //     Option::None => {
                                                //         log_warn!(
                                                //             &mut logger,
                                                //             "[TRANSCODE] Skip Frame J2C NIL"
                                                //         )
                                                //     }
                                                // }
                                            },
                                            _ => {}
                                        }
                                    })
                                    .on_tell(|name: String, _| {
                                        if !name.is_empty() {
                                            println!("[TRANSCODE - {}] Stop get frames", name);
                                            let child_ref = ctx.current().clone();
                                            let distributor =
                                                Distributor::named(format!("transcode-{}", name));
                                            distributor.unsubscribe(child_ref).map_err(|e| {
                                                println!(
                                                    "[TRANSCODE] Unsubscribe transcode child failed: {:?}",
                                                    e
                                                );
                                            });
                                            ctx.supervisor().unwrap().stop().expect("[TRANSCODE] Couldn't stop.");
                                        }
                                    })
                                    .on_fallback(|unknown, _sender_addr| {
                                        println!(
                                            "misunderstanding message: {:?}",
                                            unknown
                                        );
                                    });
                            }
                        }
                    })
            })
            .map_err(|_| {
                println!("could not create discovery children");
            })?;

        Ok(Self { supervisor_ref, children_ref })
    }

    pub fn stop(&mut self) -> Result<(), ()> {
        self.supervisor_ref.stop().expect("couldn't stop transcode");
        Ok(())
    }
}
