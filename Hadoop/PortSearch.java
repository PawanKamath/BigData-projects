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

public class PortSearch {

// Mapper class
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{
    private Text word = new Text();
    private Text harbour = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
	String[] splitString = itr.nextToken().split(",");
	String port = splitString[0];
	String routeName = splitString[2];
        word.set(port);
	harbour.set(routeName);
        context.write(word, harbour);
      }
    }
  }

//Reducer class
  public static class IntSumReducer
       extends Reducer<Text,Text,Text, Text> {
    private IntWritable result = new IntWritable();
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      
	Text routeList = new Text();
	StringBuilder routes = new StringBuilder();

      for (Text name : values) {
	String route = name.toString();
        routes.append(route).append(",");
      }
     
      routes.deleteCharAt(routes.length() - 1);
      
      result.set(routes.toString().split(",").length);

      routes.append("----- " + result);

      routeList.set(routes.toString());
      context.write(key, routeList);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(PortSearch.class);
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
