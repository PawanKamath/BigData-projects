import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.ArrayList;

public class OtherPorts {

// Mapper class
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{
    private Text port  = new Text();
    private Text route = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());

      String desiredRoute = "Chrysanthemum_Four_hundred_and_sixty-five";

      while (itr.hasMoreTokens()) {
	String[] splitString = itr.nextToken().split(",");
	String harbourName = splitString[0];
	String routeName = splitString[2];
	if (routeName.equals(desiredRoute)) {
	        harbour.set(harbourName);
		route.set(routeName);
	        context.write(route, harbour);
	}
      }
    }
  }

// Reducer class
  public static class IntSumReducer
       extends Reducer<Text,Text,Text, Text> {
    private IntWritable result = new IntWritable();
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      
	Text portList = new Text();
	StringBuilder harbours = new StringBuilder();

      for (Text name : values) {
	String harbour = name.toString();
        harbours.append(port).append(",");
      }
     
      harbours.deleteCharAt(harbours.length() - 1);
      
      portList.set(harbours.toString());
      context.write(key, portList);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(OtherHarbours.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
