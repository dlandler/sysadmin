FROM harisekhon/hbase
COPY ./conf/hbase-site.xml /hbase/conf/
RUN mkdir /hbase/scripts
COPY ./scripts/createTables.sh /hbase/scripts
RUN chmod +x /hbase/scripts/createTables.sh
