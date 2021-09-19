use bastion::prelude::*;
use log::info;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use smol::{future, Executor, Timer};
use std::{
    ops::AddAssign,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

pub struct PublisherActor {
    supervisor_ref: SupervisorRef,
    _children_ref: ChildrenRef,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EngineRequestPublish {
    pub topic: String,
    pub payload: Vec<u8>,
}

impl PublisherActor {
    pub fn start(parent_ref: &SupervisorRef, url: String) -> Result<Self, ()> {
        let supervisor_ref = parent_ref
            .supervisor(|sp| sp.with_strategy(SupervisionStrategy::OneForOne))
            .map_err(|_| {})?;
        let _children_ref = supervisor_ref
            .children(move |children| {
                children
                    .with_redundancy(1)
                    .with_distributor(Distributor::named("publisher_actor"))
                    .with_exec(move |ctx: BastionContext| {
                        let url = url.clone();

                        async move {
                            println!("[Publisher] Started.");

                            let is_closed = Arc::new(Mutex::new(false));
                            let c_is_closed = Arc::clone(&is_closed);
                            let mut client = Arc::new(
                                nats::asynk::Options::with_credentials("src/hub.creds")
                                    .close_callback(move || {
                                        *c_is_closed.lock().unwrap() = true;
                                        println!("trying to reconnect..")
                                    })
                                    .reconnect_callback(|| println!("reconnected"))
                                    .disconnect_callback(|| println!("disconnected"))
                                    .connect(&url)
                                    .await,
                            );

                            // if let Some(nc) = Arc::get_mut(&mut client) {
                            //     nc.as_ref()
                            //         .unwrap()
                            //         .flush_timeout(Duration::from_millis(2000))
                            //         .await
                            //         .unwrap();
                            // }

                            let ex = Executor::new();
                            let c_client = Arc::clone(&client);
                            ex.spawn(async move {
                                loop {
                                    if *is_closed.lock().unwrap() {
                                        let c_is_closed = Arc::clone(&is_closed);
                                        *c_is_closed.lock().unwrap() = false;
                                        let mut c_client = Arc::clone(&c_client);
                                        if let Some(c_client) = Arc::get_mut(&mut c_client) {
                                            *c_client = nats::asynk::Options::with_credentials(
                                                "src/hub.creds",
                                            )
                                            .close_callback(move || {
                                                *c_is_closed.lock().unwrap() = true;
                                                println!("trying to reconnect..")
                                            })
                                            .reconnect_callback(|| println!("reconnected"))
                                            .disconnect_callback(|| println!("disconnected"))
                                            .connect(&url)
                                            .await;

                                            // c_client
                                            //     .as_ref()
                                            //     .unwrap()
                                            //     .flush_timeout(Duration::from_millis(2000))
                                            //     .await
                                            //     .unwrap();
                                        }
                                    }
                                    Timer::after(Duration::from_secs(30)).await;
                                }
                            })
                            .detach();
                            thread::spawn(move || {
                                future::block_on(ex.run(future::pending::<()>()))
                            });

                            println!("Client: {:?}", client);
                            let sequence: Arc<Mutex<i32>> = Arc::new(Mutex::new(0));

                            loop {
                                let pub_client = client.clone();
                                let sequence = sequence.clone();
                                MessageHandler::new(ctx.recv().await?)
                                    //publish
                                    .on_tell(|message: EngineRequestPublish, _sender| {
                                        //println!("{:?}", message.topic);
                                        let payload: Value =
                                            serde_json::from_slice(&message.payload).unwrap();
                                        // info!(
                                        //     "[BEFORE] Cam id {} with sequence {}",
                                        //     payload["cam_id"], payload["timestamp"]
                                        // );
                                        // if message.payload.len() <= 1024 * 1024 {
                                        run!(async move {
                                            match pub_client
                                                .as_ref()
                                                .as_ref()
                                                .unwrap()
                                                .publish(&message.topic, message.payload.clone())
                                                .await
                                            {
                                                Ok(_) => {
                                                    // info!(
                                                    //     "[AFTER] Cam id {} with sequence {}",
                                                    //     payload["cam_id"], payload["timestamp"]
                                                    // );
                                                    // println!(
                                                    //     "count: {}",
                                                    //     *sequence.lock().unwrap()
                                                    // );
                                                    // *sequence.lock().unwrap() += 1;
                                                }
                                                Err(err) => {}
                                            };
                                        });
                                        // } else {
                                        //     println!(
                                        //         "[Publisher]: Over 1 MB payload: {}",
                                        //         message.topic
                                        //     );
                                        // }
                                    });
                            }
                        }
                    })
            })
            .map_err(|_| {
                println!("could not create publisher children");
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
