mod aes;
mod config;
pub mod constants;
pub mod jp2k;
pub mod protos;
mod publisher_actor;
mod test_actor;
pub mod test_transcode_actor;
mod throttle;
mod transcode_actor;

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
use crate::test_transcode_actor::TestTranscodeActor;
use crate::transcode_actor::TranscodeEngine;

fn main() {
    let config = config::Config::from_args(std::env::args()).unwrap();

    Bastion::init();
    Bastion::start();

    //let url = "10.50.13.185:4222".to_string();
    //let url = "10.50.13.181:4222".to_string();
    let url = config.get_nats_url();
    let test_redundancy = config.get_redundancy();
    let publish_actors = config.get_publisher_actors();
    let max_msg = config.get_max_msg();
    let max_cams = config.get_max_cams();
    let fps = config.get_fps();
    let j2c_host = config.get_j2c_host();
    println!(
        "url: {}, Cams: {}, Publisher: {}, total Msg/Cam: {}, fps: {}",
        url, max_cams, publish_actors, max_msg, fps
    );

    simple_logging::log_to_file("logs/log.txt", LevelFilter::Info).unwrap();
    let parent_ref = Bastion::supervisor(|sp| sp.with_strategy(SupervisionStrategy::OneForOne))
        .expect("could not create a supervisor");

    for n in 0..publish_actors {
        let name = format!("publish_actor_{}", n);
        let _ = PublisherActor::start(&parent_ref, url.clone(), name);
        sleep(std::time::Duration::from_millis(10));
    }

    for i in 1..=max_cams {
        let j2c_host = j2c_host.clone();
        let cam_id = format!("cam_{}", i);
        TranscodeEngine::init(
            &parent_ref.clone(),
            publish_actors,
            format!("transcode-{}", cam_id.clone()).as_str(),
            1,
            j2c_host,
        )
        .unwrap();
        sleep(std::time::Duration::from_secs(1));
        let parent_ref = parent_ref.clone();
        run!(async move {
            println!("{}", cam_id);
            let _ = TestTranscodeActor::start(
                &parent_ref,
                cam_id,
                test_redundancy,
                max_msg,
                fps,
                publish_actors,
            );
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
