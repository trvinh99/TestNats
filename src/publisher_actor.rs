use bastion::prelude::*;
use log::{info, warn};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use smol::{future, Executor, Timer};
use std::time::SystemTime;
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
    pub fn start(parent_ref: &SupervisorRef, url: String, name: String) -> Result<Self, ()> {
        let supervisor_ref = parent_ref
            .supervisor(|sp| sp.with_strategy(SupervisionStrategy::OneForOne))
            .map_err(|_| {})?;
        let _children_ref = supervisor_ref
            .children(move |children| {
                children
                    .with_redundancy(1)
                    .with_distributor(Distributor::named(name.clone()))
                    .with_exec(move |ctx: BastionContext| {
                        let url = url.clone();
                        println!("[{}] Started.", name);

                        async move {

                            let is_closed = Arc::new(Mutex::new(false));
                            let c_is_closed = Arc::clone(&is_closed);
                            let mut client = Arc::new(
                                //nats::asynk::Options::with_credentials("data/stage.creds")
                                nats::asynk::Options::with_user_pass("leaf","changeme")
                                    .close_callback(move || {
                                        *c_is_closed.lock().unwrap() = true;
                                        println!("1 trying to reconnect..")
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
                                                "data/stage.creds",
                                            )
                                            .close_callback(move || {
                                                *c_is_closed.lock().unwrap() = true;
                                                println!("2 trying to reconnect..")
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
                                        let now = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)
                                            {
                                                Ok(n) => n.as_millis(),
                                                Err(_) => panic!("SystemTime before UNIX EPOCH!"),
                                            };
                                        let timestampstr = payload["timestamp"].as_str().unwrap().to_string();                                                   
                                        let timestamp =  timestampstr.parse::<u128>().unwrap();
                                        
                                        let delay = now - timestamp;
                                        if delay > 1000 {

                                            warn!(
                                                "[BEFORE PUBLISH][LongDelay] Cam id: {}, Sequence: {}, Timestamp: {}, Delay: {}",
                                                payload["cam_id"].as_str().unwrap().to_string(), payload["seq"], payload["timestamp"].as_str().unwrap().to_string(),delay
                                            );

                                        }
                                        else {

                                            info!(
                                                "[BEFORE PUBLISH][OK] Cam id: {}, Sequence: {}, Timestamp: {}, Delay: {}",
                                                payload["cam_id"].as_str().unwrap().to_string(), payload["seq"], payload["timestamp"].as_str().unwrap().to_string(),delay
                                            );

                                        }
                                        
                                         if message.payload.len() <= 1024 * 1024 {
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
                                                    //     "[Published] Cam id: {}, payload length: {}",
                                                    //     payload["cam_id"], message.payload.len()
                                                    // );
                                                    let now1 = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)
                                                    {
                                                        Ok(n) => n.as_millis(),
                                                        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
                                                    };
                                                  
                                                    let delay1 = now1 - now;

                                                    if delay1 > 1000 {
                                                        warn!(
                                                            "[Published][LongDelay] Cam id: {}, Sequence: {}, Timestamp: {}, Delay: {}, Payload length: {}",
                                                            payload["cam_id"].as_str().unwrap().to_string(), payload["seq"], payload["timestamp"].as_str().unwrap().to_string(), delay1, message.payload.len()
                                                        );
                                                    }
                                                    else {

                                                        info!(
                                                            "[Published][OK] Cam id: {} Sequence: {}, Timestamp: {}, Delay: {}, Payload length: {}",
                                                            payload["cam_id"].as_str().unwrap().to_string(), payload["seq"], payload["timestamp"].as_str().unwrap().to_string(), delay1, message.payload.len()
                                                        );

                                                    }
                                                    // println!(
                                                    //     "count: {}",
                                                    //     *sequence.lock().unwrap()
                                                    // );
                                                    // *sequence.lock().unwrap() += 1;
                                                }
                                                Err(err) => {}
                                            };
                                        });
                                        } else {
                                            println!(
                                                "[Publisher]: Over 1 MB payload: {}",
                                                message.topic
                                            );
                                        }
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
