#!/bin/bash -e

echo "[start toxiproxy]"
nohup toxiproxy -port 18474 > /tmp/toxiproxy.log 2>&1 &
sleep 3
echo "[toxiproxy logs]"
cat /tmp/toxiproxy.log
echo "[toxiproxy process]"
ps axu | grep toxiproxy