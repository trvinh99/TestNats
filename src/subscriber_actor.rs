
use bastion::prelude::*;
use nats::asynk::Connection;


pub struct SubscriberActor {
     supervisor_ref: SupervisorRef,
     children_ref: ChildrenRef,
}
#[derive(Debug, Clone)]
pub struct EngineRequestSubscribe {
    pub distributor_name: String,
    pub topic: String,
}

impl SubscriberActor {
    pub fn start(parent_ref: &SupervisorRef, client: Connection) -> Result<Self, ()> {
        let supervisor_ref =
            parent_ref.supervisor(|sp| sp.with_strategy(SupervisionStrategy::OneForOne))
                .map_err(|_| {
                    println!("could not create subscriber supervisor")})?;

        let children_ref = supervisor_ref.children( move |children| {
            children
                .with_redundancy(1)
                .with_distributor(Distributor::named("subscriber_actor"))
                .with_exec( move |ctx: BastionContext| {
                    let client = client.clone();
                    
                    async move {
                        println!("[Subscriber] Started.");
                        


                        println!("Client {:?}", client);


                        loop {
                            let sub_client = client.clone();
                            MessageHandler::new(ctx.recv().await?)
                                //subscibe
                                .on_tell(|message: EngineRequestSubscribe, _sender|  {
                                    println!("[Subscriber] Receive msg from ENGINE {:?}!", message);
                                    spawn!(async move {
                                        //log_info!(&mut logger, "[Subscriber] Subcribe receive topic {}!", message.topic);
                                        //println!("[Subscriber] Subcribe receive topic {}!", message.topic);

                                        let topic = format!("vb.hub");
                                        // .{}.{}", hub_id, &message.topic
                                        let sub = sub_client.subscribe(&topic).await.unwrap();

                                        loop {
                                            match sub.next().await {
                                                Some(msg) => {
                                                    println!("Message: {:?}", msg.clone().data);
                                                    let sender = Distributor::named(&message.distributor_name);
                                                    match sender.tell_one(msg.data) {
                                                        Ok(_) => {
                                                            println!("[Subscriber] Sending success")
                                                        },
                                                        // Err(msg) => {log_err!(&mut logger, "[Subscriber] Error send message: {}", msg);tracing::error!("[Subscriber] Error send message: {}", msg)}
                                                        Err(msg) => {
                                                            
                                                        }
                                                    };
                                                },
                                                None => {
                                                    println!("[Subscriber] Subscribe failed")}
                                            };   
                                        }
                                    });
                                });
                        }; 
                    }
                })
            })
            .map_err(|_| {
                println!("could not create subscriber children")
            })?;
        
        Ok(Self {
            supervisor_ref,
            children_ref,
        })
    }
    pub async fn stop(&mut self) -> Result<(), ()> {
        self.supervisor_ref.stop()
    }
}

// let subject_map = subject_map.clone();
// let map = subject_map.clone();
// printlnln!("[NATS] Client {:?}!", client);
// let mut subs = vec![];

//     for item in subject_map.iter() {
//         subs.push(item.key().clone());
//     }
//     loop{
//     for sub in subs {
//         printlnln!("[NATS] Receive topic {}!", sub);
//         let client = client.clone();
//         let subject_map = subject_map.clone();
//         spawn!(async move {
//             let client = client.clone();
//             let subcription = client.subscribe(&sub).await.unwrap();
//             loop {
//                     let msg = subcription.next().await.unwrap();
//                     printlnln!("[NATS] Receive msg {:?}!", msg);

//                     let groups_target = subject_map.get(&sub).unwrap();
//                     for group_name in &*groups_target {
//                         let msg = msg.clone();

//                         let sender = Distributor::named(&group_name);

//                         let resp = String::from_utf8(msg.data).unwrap();

//                         sender.tell_one(resp).unwrap();

//                     }

//                 }  
                    

//         });
//     }
//     loop {
//         let map = map.clone();
//         MessageHandler::new(ctx.recv().await?)
//                 .on_tell(move |message: EngineRequest, _sender|  {
//                     printlnln!("[ENGINE ->NATS] Engine send topic {}!", message.topic);
//                     spawn!(async move {
//                         if map.contains_key(&message.topic) {
//                             let mut topic = map.get_mut(&message.topic).unwrap();
//                             if !topic.contains(&message.distributor_name) {
//                                 topic.push(message.distributor_name.clone());
//                             }
//                         }
//                         else {
//                             let mut new_vec = Vec::new();
//                             new_vec.push(message.distributor_name.clone());
//                             map.insert(message.topic, new_vec);
                            
//                         } 
//                         printlnln!("Map {:?}", map);
//                     })
                
//         });
//     }
// }