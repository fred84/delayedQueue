#!/bin/bash -e

curl --silent -L https://github.com/Shopify/toxiproxy/releases/download/v2.1.4/toxiproxy-server-linux-amd64 -o ./bin/toxiproxy-server

echo "[start toxiproxy]"
chmod +x ./bin/toxiproxy-server
nohup bash -c "./bin/toxiproxy-server > /tmp/toxiproxy.log 2>&1 &"