#!/bin/bash
cat /dev/null > logs/log.txt
totalCams=30
#totalCams=1
totalMsg=3000
url=tls://stage-nats.lexray.com:4222
fps=3
./target/release/TestNats --max-msg=$totalMsg --publisher-actors=1 --max-cams=$totalCams  --nats-url=$url --fps=$fps
