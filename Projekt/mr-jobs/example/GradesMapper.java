package de.jofre.grades;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class GradesMapper
  extends Mapper<Text, Text, IntWritable, IntWritable>
{
  private static final Logger log = Logger.getLogger(GradesMapper.class.getName());
  
  private IntWritable year_int = null;
  private IntWritable grade_int = null;
  
  public GradesMapper() {}
  
  public void map(Text key, Text value, Mapper<Text, Text, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException
  {
    if (key.toString().length() == 16) {
      String year_str = key.toString().substring(10, 14);
      String grade_str = key.toString().substring(14, 16);
      
      year_int = new IntWritable(Integer.parseInt(year_str));
      grade_int = new IntWritable(Integer.parseInt(grade_str));
      

      context.write(year_int, grade_int);
    } else {
      log.log(Level.INFO, "Ung�ltige Datensatzl�nge entdeckt (" + key.toString().length() + ").");
    }
  }
}
