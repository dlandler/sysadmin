package main;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HbaseReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
	    throws IOException, InterruptedException {
	int i = 0;
	for (IntWritable val : values) {
	    i += val.get();
	}
	context.write(key, new IntWritable(i));
    }
}