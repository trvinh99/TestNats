#!/bin/bash
echo "Send actor OK:"
cat logs/log.txt  | grep "BEFORE PUBLISH\]\[OK\]" | wc -l
echo "Send actor delay:"
cat logs/log.txt  | grep "BEFORE PUBLISH\]\[LongDelay\]" | wc -l
echo "Publish OK:"
cat logs/log.txt  | grep "Published\]\[OK\]" | wc -l
echo "Publish Delay:"
cat logs/log.txt  | grep "Published\]\[LongDelay\]" | wc -l
echo "Total Msg:"
cat logs/log.txt  | grep "Published" | wc -l
