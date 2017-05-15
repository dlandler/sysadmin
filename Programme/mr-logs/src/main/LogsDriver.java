package main;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;

public class LogsDriver extends Configured implements Tool {
    public LogsDriver() {
    }

    private static final Logger log = Logger.getLogger(LogsDriver.class.getName());

    public static void main(String[] args) {
	int res = 1;
	try {
	    res = org.apache.hadoop.util.ToolRunner.run(new Configuration(), new LogsDriver(), args);
	} catch (Exception e) {
	    log.log(Level.SEVERE, "Fehler beim Ausführen des Jobs!");
	    e.printStackTrace();
	}
	System.exit(res);
    }

    public int run(String[] args) {
	log.log(Level.INFO, "Starte Map-Reduce-Job 'Logs Driver'...");

	Configuration conf = getConf();

	Job job = null;
	try {
	    job = Job.getInstance(conf);
	} catch (IOException e1) {
	    log.log(Level.SEVERE, "Fehler bei Instanzierung des Jobs!");
	    e1.printStackTrace();
	}

	job.setJarByClass(LogsDriver.class);

	job.setMapperClass(LogsMapper.class);
	job.setReducerClass(LogsReducer.class);

	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(Text.class);
	job.setInputFormatClass(KeyValueTextInputFormat.class);
	job.setOutputFormatClass(NullOutputFormat.class);

	try {
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	} catch (IllegalArgumentException e) {
	    log.log(Level.SEVERE, "Fehler (Argument) beim Setzen des Eingabepfades!");
	    e.printStackTrace();
	} catch (IOException e) {
	    log.log(Level.SEVERE, "Fehler (IO) beim Setzen des Eingabepfades!");
	    e.printStackTrace();
	}

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
