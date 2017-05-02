package main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class GradesReducer extends Reducer<IntWritable, IntWritable, IntWritable, FloatWritable> {
    public GradesReducer() {
    }

    protected void reduce(IntWritable key, Iterable<IntWritable> values,
	    Reducer<IntWritable, IntWritable, IntWritable, FloatWritable>.Context context)
	    throws IOException, InterruptedException {
	float sum = 0.0F;
	int count = 0;

	for (IntWritable val : values) {
	    sum += val.get();
	    count++;
	}

	float result = sum / count;

	result /= 10.0F;

	writeToHbase(key, new FloatWritable(result));

	context.write(key, new FloatWritable(result));
    }

    private void writeToHbase(IntWritable key, FloatWritable result) throws IOException {
	Configuration conf = HBaseConfiguration.create();
	conf.set("hbase.zookeeper.property.clientPort", "2181");
	conf.set("hbase.zookeeper.quorum", "hbase");
	HTable table = new HTable(conf, "grades");
	Put put = new Put(Bytes.toBytes(key.toString()));
	put.add(Bytes.toBytes("mark"), null, Bytes.toBytes(result.toString()));
	table.put(put);
	table.close();
    }
}
