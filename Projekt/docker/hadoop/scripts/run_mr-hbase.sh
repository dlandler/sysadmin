#!/bin/bash

cd ..
rm -r mr-output-hbase
bin/hadoop jar mr-jobs/mr-hbase.jar file:/usr/local/hadoop/mr-output-hbase
