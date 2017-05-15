FROM sequenceiq/hadoop-docker
RUN mkdir /usr/local/hadoop/mr-jobs
RUN mkdir /usr/local/hadoop/mr-input
RUN mkdir /usr/local/hadoop/scripts
COPY ./scripts/run_mr-logs.sh /usr/local/hadoop/scripts
COPY ./scripts/run_mr-hbase.sh /usr/local/hadoop/scripts
RUN chmod +x /usr/local/hadoop/scripts/run_mr-logs.sh
RUN chmod +x /usr/local/hadoop/scripts/run_mr-hbase.sh
