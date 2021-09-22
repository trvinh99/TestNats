mod publisher_actor;
mod test_actor;
mod throttle;

use bastion::supervisor::SupervisionStrategy;
use bastion::Bastion;
use bastion::{run, spawn};
use log::LevelFilter;
use log::{info, trace, warn};
use openssl::rsa::Padding;
use openssl::rsa::Rsa;
use publisher_actor::PublisherActor;
use serde_json::json;
use serde_json::Value;
use smol::future;
use smol::Executor;
use smol::Timer;
use std::fs::File;
use std::io::Read;
use std::io::Write;
use std::thread::{self, sleep};
use std::time::Duration;
use tokio::task;

use crate::test_actor::TestActor;

fn main() {
    Bastion::init();
    Bastion::start();

    //let url = "10.50.13.185:4222".to_string();
    //let url = "10.50.13.181:4222".to_string();
    let url = "dev.lexray.com:60064".to_string();
    let test_redundancy = 1;
    let max_msg = 300;
    let max_cams = 24;
    let fps = 5;

    simple_logging::log_to_file("logs/log.txt", LevelFilter::Info).unwrap();
    let parent_ref = Bastion::supervisor(|sp| sp.with_strategy(SupervisionStrategy::OneForOne))
        .expect("could not create a supervisor");
    
    for n in 0..test_redundancy {
        let name =  format!("publisher_actor_{}", n);
        let _ = PublisherActor::start(&parent_ref, url, name);
        sleep(std::time::Duration::from_secs(1));
    }

    for i in 1..=max_cams {
        let parent_ref = parent_ref.clone();
        run!(async move {
            let cam_id = format!("cam_{}", i);
            println!("{}", cam_id);
            let _ = TestActor::start(&parent_ref, cam_id, test_redundancy, max_msg, fps);
            sleep(std::time::Duration::from_millis(10));
        });
    }

    // spawn!(async move {
    //     simple_logging::log_to_file("src/log.txt", LevelFilter::Info).unwrap();
    //     let url = "nats://dev.lexray.com:60064";
    //     let client = nats::asynk::Options::with_credentials("src/hub.creds")
    //         .connect(&url)
    //         .await
    //         .unwrap();

    //     let topic = "lexhub.test";
    //     // let sub = client.subscribe("lexray.hub.3a9d1f15-6162-4201-a275-cad076f47ba7.live.8d0fa59d-f02b-440a-9158-4199809dde09.timestamp").await.unwrap();
    //     let mut sequence = 0;

    //     let mut file = File::open("src/image.jpg").unwrap();
    //     let mut contents = [];
    //     let frame = file.read(&mut contents).unwrap();

    //     loop {
    //         let payload = json!({
    //             "cam_id": "123",
    //             "frame": frame,
    //             "timestamp": sequence,
    //         });
    //         client
    //             .publish(&topic, serde_json::to_vec(&payload).unwrap())
    //             .await
    //             .unwrap();
    //         sequence += 1;
    //         // let msg = sub.next().await.unwrap();
    //         // let a: String = String::from_utf8(msg.data).unwrap();
    //         // println!("Msg: {:?}", a);
    //         // info!("{}", a);
    //     }
    // });

    Bastion::block_until_stopped();
}
