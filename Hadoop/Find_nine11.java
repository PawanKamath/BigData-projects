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

public class Find_nine11 {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    private Text route = new Text();
    private Text nmbr = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
	String[] splitString = itr.nextToken().split(",");
	String routeName = splitString[2];
	String routeNumber = splitString[3];
        route.set(routeName);
	number.set(routeNumber);
        context.write(route, number);
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text, Text> {
    private IntWritable result = new IntWritable();
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      
	Text harbourList = new Text();
	StringBuilder harbours = new StringBuilder();

      for (Text nmbr : values) {
	String routeNumber = number.toString();
	System.out.println(routeNumber);
	String name = key.toString();
// search for routes starting with "911".
	if (routeNumber.startsWith("911")) {
	        harbours.append(routeNumber).append(",");
	}
      }
     

      if (harbours.length() > 0) {
	      harbourList.set(harbours.substring(0, harbours.length() - 1));
	      context.write(key, harbourList);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(NineOneOneFinder.class);
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
