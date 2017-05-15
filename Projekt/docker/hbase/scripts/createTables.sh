#!/bin/bash

cd ..
echo "create 'metadata', 'keycounter'" | bin/hbase shell
echo "create 'logs', 'ip', 'date', 'time', 'request', 'statuscode', 'referer', 'browser'" | bin/hbase shell
