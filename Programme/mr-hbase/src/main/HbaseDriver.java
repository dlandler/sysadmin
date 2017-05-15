package main;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class HbaseDriver extends Configured implements Tool {
    public HbaseDriver() {
    }

    private static final Logger log = Logger.getLogger(HbaseDriver.class.getName());

    public static void main(String[] args) {
	int res = 1;
	try {
	    res = org.apache.hadoop.util.ToolRunner.run(new Configuration(), new HbaseDriver(), args);
	} catch (Exception e) {
	    log.log(Level.SEVERE, "Fehler beim Ausführen des Jobs!");
	    e.printStackTrace();
	}
	System.exit(res);
    }

    public int run(String[] args) throws IOException {
	log.log(Level.INFO, "Starte Map-Reduce-Job 'Logs-HBase Driver'...");

	Configuration conf = HBaseConfiguration.create();
	conf.set("hbase.zookeeper.property.clientPort", "2181");
	conf.set("hbase.zookeeper.quorum", "hbase");

	Job job = null;
	try {
	    job = Job.getInstance(conf);
	} catch (IOException e1) {
	    log.log(Level.SEVERE, "Fehler bei Instanzierung des Jobs!");
	    e1.printStackTrace();
	}

	job.setJarByClass(HbaseDriver.class);

	Scan scan = new Scan();
	scan.setCaching(500);
	scan.setCacheBlocks(false);

	TableMapReduceUtil.initTableMapperJob(TableName.valueOf("logs"), scan, HbaseMapper.class, Text.class,
		IntWritable.class, job);

	job.setReducerClass(HbaseReducer.class);
	job.setNumReduceTasks(1);

	FileOutputFormat.setOutputPath(job, new Path(args[0]));

	boolean result = false;

	try {
	    result = job.waitForCompletion(true);
	} catch (ClassNotFoundException e) {
	    log.log(Level.SEVERE, "Fehler (ClassNotFound) beim Ausführen des Jobs!");
	    e.printStackTrace();
	} catch (IOException e) {
	    log.log(Level.SEVERE, "Fehler (IOException) beim Ausführen des Jobs!");
	    e.printStackTrace();
	} catch (InterruptedException e) {
	    log.log(Level.SEVERE, "Fehler (Interrupted) beim Ausführen des Jobs!");
	    e.printStackTrace();
	}

	log.log(Level.INFO, "Fertig!");
	return result ? 0 : 1;
    }
}
