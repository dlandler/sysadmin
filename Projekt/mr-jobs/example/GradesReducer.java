package de.jofre.grades;

import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class GradesReducer
  extends Reducer<IntWritable, IntWritable, IntWritable, FloatWritable>
{
  public GradesReducer() {}
  
  protected void reduce(IntWritable key, Iterable<IntWritable> values, Reducer<IntWritable, IntWritable, IntWritable, FloatWritable>.Context context)
    throws IOException, InterruptedException
  {
    float sum = 0.0F;
    int count = 0;
    
    for (IntWritable val : values) {
      sum += val.get();
      count++;
    }
    

    float result = sum / count;
    

    result /= 10.0F;
    

    context.write(key, new FloatWritable(result));
  }
}
