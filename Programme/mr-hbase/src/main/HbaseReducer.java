package main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HbaseReducer extends Reducer<Text, IntWritable, Text, FloatWritable> {

    Configuration conf = null;
    HTable table;
    int rowCount;

    public void setup(Context context) throws IOException {
	conf = context.getConfiguration();
	table = new HTable(conf, "logs");
	Scan scan = new Scan();
	// Scanning the required columns
	scan.addColumn(Bytes.toBytes("ip"), null);

	// Getting the scan result
	ResultScanner scanner = table.getScanner(scan);

	for (Result result = scanner.next(); result != null; result = scanner.next()) {
	    rowCount++;
	}
	scanner.close();
    }

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
	    throws IOException, InterruptedException {
	float i = 0;
	for (IntWritable val : values) {
	    i += val.get();
	}

	float percent = (i / rowCount) * 100;
	String strPercent = String.format("%.2f", percent);
	percent = Float.parseFloat(strPercent);

	context.write(key, new FloatWritable(percent));
    }
}