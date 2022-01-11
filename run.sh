#!/bin/bash
cat /dev/null > logs/log.txt
#./target/release/TestNats --max-msg=3000 --publisher-actors=1 --max-cams=30
fps=5
totalCams=15
totalMsg=100
j2cHost=10.50.31.168
natsUrl=nats://10.50.13.213:4222
publisherActors=4
echo "Running total-cams $totlCams totalmsg; $totalMsg natsUrl: $natsUrl"
./target/release/TestNats --redundancy=1 --publisher-actors=$publisherActors --max-msg=$totalMsg --max-cams=$totalCams --fps=$fps --j2c-host=$j2cHost  --nats-url=$natsUrl
