use bastion::prelude::*;
use log::info;
use serde::{Deserialize, Serialize};
use serde_json::json;
use smol::{future, Executor, Timer};
use std::{
    fs::File,
    io::Read,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use crate::publisher_actor::EngineRequestPublish;

pub struct TestActor {
    supervisor_ref: SupervisorRef,
    _children_ref: ChildrenRef,
}

impl TestActor {
    pub fn start(
        parent_ref: &SupervisorRef,
        cam_id: String,
        test_redundancy: usize,
    ) -> Result<Self, ()> {
        let supervisor_ref = parent_ref
            .supervisor(|sp| sp.with_strategy(SupervisionStrategy::OneForOne))
            .map_err(|_| {})?;
        let test_redundancy = test_redundancy.clone();

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

                        async move {
                            loop {
                                let payload = json!({
                                    "cam_id": cam_id,
                                    "image": contents,
                                    "timestamp": sequence,
                                });
                                let topic = format!("lexhub.test.{}", cam_id);
                                let msg = EngineRequestPublish {
                                    topic: topic,
                                    payload: serde_json::to_vec(&payload).unwrap(),
                                };
                                // info!(
                                //     "[TEST_ACTOR] Cam id {} with sequence {}",
                                //     payload["cam_id"], payload["timestamp"]
                                // );

                                let nats = Distributor::named("publisher_actor");
                                nats.tell_one(msg).expect("Can't send the message!");

                                if sequence > 10 {
                                    break;
                                }

                                sequence += 1;
                            }
                            Ok(())
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
    pub async fn stop(&mut self) -> Result<(), ()> {
        self.supervisor_ref.stop()
    }
}
