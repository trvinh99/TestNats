#!/bin/bash
cat /dev/null > logs/log.txt
totalCams=20
#totalCams=1
totalMsg=2000
totalMsg=100
url=tls://nats.lexray.com:4222
fps=3
./target/release/TestNats --max-msg=$totalMsg --publisher-actors=1 --max-cams=$totalCams  --nats-url=$url --fps=$fps
