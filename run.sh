#!/bin/bash
cat /dev/null > logs/log.txt
./target/release/TestNats --max-msg=3000 --publisher-actors=1 --max-cams=30
