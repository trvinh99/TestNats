
use bastion::prelude::*;
use dashmap::DashMap;
use log::info;

use serde_json::json;
use smol::{future, Executor, Timer};
use std::time::SystemTime;
use std::{
    fs::File,
    io::Read,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use crate::publisher_actor::EngineRequestPublish;
use crate::subscriber_actor::EngineRequestSubscribe;
use crate::throttle::Throttle;

pub struct TestActor {
    supervisor_ref: SupervisorRef,
    _children_ref: ChildrenRef,
}

impl TestActor {
    pub fn start(
        parent_ref: &SupervisorRef,
        cam_id: String,
        test_redundancy: usize,
        max_msg: usize,
        fps: usize,
        number_publish_worker: usize,
    ) -> Result<Self, ()> {
        let supervisor_ref = parent_ref
            .supervisor(|sp| sp.with_strategy(SupervisionStrategy::OneForOne))
            .map_err(|_| {})?;
        let test_redundancy = test_redundancy.clone();
        let camera_source_map: Arc<DashMap<String, String>> = Arc::new(DashMap::new());
        let nats = Distributor::named("subscriber_actor");
        let subscriber = EngineRequestSubscribe {
            distributor_name: "discovery".to_owned(),
            topic: "test_actor".to_owned(),
        };
        nats.tell_one(subscriber)
            .expect("[TEST ACTOR] could not subscribe to topic \"test_actor\"");

        let _children_ref = supervisor_ref
            .children(move |children| {
                children
                    .with_redundancy(test_redundancy)
                    .with_distributor(Distributor::named("test_actor"))
                    .with_exec(move |ctx: BastionContext| {
                        let cam_id = cam_id.clone();
                        let mut sequence = 0;

                        let mut file = File::open("src/test_frame.txt").unwrap();
                        let mut contents = vec![];
                        file.read_to_end(&mut contents).unwrap();
                        info!(
                            "[Starting actor] Cam id: {}, Total msg: {}",
                            cam_id, max_msg
                        );

                        let mut camera_source_map_child = camera_source_map.clone();
                        async move {
                            let mut throttle =
                                Throttle::new(std::time::Duration::from_secs(1), fps);
                            let distr_name = loadbalance_publisher(
                                camera_source_map_child.clone(),
                                cam_id.clone(),
                                number_publish_worker,
                            );
                            //for x in 1..10
                            loop {
                                // let result = throttle.accept();
                                // match result {
                                // Ok(_) => {
                                info!(
                                    "[Starting actor] Cam id: {}, sequence: {}",
                                    cam_id, sequence
                                );
                                TestActor::publish_nats(
                                    cam_id.clone(),
                                    contents.clone(),
                                    sequence,
                                    distr_name.clone(),
                                );
                                if sequence == max_msg - 1 {
                                    break Ok(());
                                    //return;
                                }
                                sequence += 1;
                                //     }
                                //     Err(_) => {}
                                // }

                                Timer::after(Duration::from_millis(200)).await;
                            }
                            //Ok(());
                        }
                    })
            })
            .map_err(|_| {
                println!("could not create test children");
            })?;

        Ok(Self {
            supervisor_ref,
            _children_ref,
        })
    }

    pub fn publish_nats(cam_id: String, contents: Vec<u8>, sequence: usize, distr_name: String) {
        let now = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
            Ok(n) => n.as_millis(),
            Err(_) => panic!("SystemTime before UNIX EPOCH!"),
        };
        let payload = json!({
            "cam_id": cam_id,
            "image": contents,
            "seq": sequence,
            "timestamp": now.to_string(),
        });
        let topic = format!("lexhub.test.{}", cam_id);
        let msg = EngineRequestPublish {
            topic: topic,
            payload: serde_json::to_vec(&payload).unwrap(),
        };
        info!(
            "[SendToPublisher] Cam id: {}, Sequence: {}, Timestamp: {}",
            payload["cam_id"].as_str().unwrap().to_string(),
            payload["seq"],
            payload["timestamp"].as_str().unwrap().to_string()
        );
        // info!(
        //     "[SendToPublisher] Cam id: {}, Sequence: {}, Timestamp: {}",
        //     payload["cam_id"].as_str().unwrap().to_string(), payload["seq"].as_str().unwrap().to_string(), payload["timestamp"].as_str().unwrap().to_string()
        // );

        let nats = Distributor::named(distr_name);
        nats.tell_one(msg).expect("Can't send the message!");
    }

    pub async fn stop(&mut self) -> Result<(), ()> {
        self.supervisor_ref.stop()
    }
}

fn loadbalance_publisher(
    source: Arc<DashMap<String, String>>,
    cam_id: String,
    number_publish_worker: usize,
) -> String {
    if source.contains_key(&cam_id) {
        return (*source.get(&cam_id).unwrap()).clone();
    }
    let index = fastrand::usize(0..number_publish_worker);

    let name = format! {"publish_actor_{}",index};
    source.insert(cam_id, name.clone());
    name
}
