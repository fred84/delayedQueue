#!/bin/bash -e

echo "[start toxiproxy]"
chmod +x ./bin/toxiproxy-server
nohup toxiproxy -port 18474 > /tmp/toxiproxy.log 2>&1 &
sleep 3
echo "[toxiproxy logs]"
cat /tmp/toxiproxy.log
echo "[toxiproxy process]"
ps axu | grep toxiproxy