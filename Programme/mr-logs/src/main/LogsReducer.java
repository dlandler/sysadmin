package main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LogsReducer extends Reducer<Text, Text, Text, Text> {
    public LogsReducer() {
    }

    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
	    throws IOException, InterruptedException {

	String sValues = null;
	Text textValues = null;
	String[] columns = null;

	for (Text val : values) {
	    sValues = val.toString();
	    textValues = val;
	}

	// 0: Date, 1: Time, 2: Request, 3: Statuscode, 4: Referer, 5: Browser
	columns = sValues.split(";");

	writeToHbase(key.toString(), columns);

	context.write(key, textValues);
    }

    private void writeToHbase(String key, String[] columns) throws IOException {
	Configuration conf = HBaseConfiguration.create();
	conf.set("hbase.zookeeper.property.clientPort", "2181");
	conf.set("hbase.zookeeper.quorum", "hbase");
	HTable table = new HTable(conf, "logs");
	Put put = new Put(Bytes.toBytes(key));
	put.add(Bytes.toBytes("date"), null, Bytes.toBytes(columns[0]));
	put.add(Bytes.toBytes("time"), null, Bytes.toBytes(columns[1]));
	put.add(Bytes.toBytes("request"), null, Bytes.toBytes(columns[2]));
	put.add(Bytes.toBytes("statuscode"), null, Bytes.toBytes(columns[3]));
	put.add(Bytes.toBytes("referer"), null, Bytes.toBytes(columns[4]));
	put.add(Bytes.toBytes("browser"), null, Bytes.toBytes(columns[5]));
	table.put(put);
	table.close();
    }
}
