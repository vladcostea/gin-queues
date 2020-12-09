#!/bin/sh

url=http://localhost:8080/ping

wrk --duration 15s --threads 10 --connections 2000 --script wrk.lua $url
