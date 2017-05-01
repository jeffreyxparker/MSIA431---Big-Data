//package mr_app.Homework2;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.*;

public class Exercise3 extends Configured implements Tool {
	
	//Mapper
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {
    	// Configure and setup
		public void configure(JobConf job) {}
		//protected void setup(OutputCollector<Text, DoubleWritable> output) throws IOException, InterruptedException {}
		
		public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
			try {
				String line = value.toString();
				String[] elements = line.split(",");
				int year= Integer.parseInt(line.split(",")[165]);
				String title = line.split(",")[1];
				String artist = line.split(",")[2];
				String duration = line.split(",")[3];
				String tuple = title+","+artist+","+duration+","+year;
				double smiley = 0.0;

				if(year >= 2000 && year <= 2010) {
					output.collect(new Text(tuple), new DoubleWritable(smiley));
		    	}
			}
			catch (Exception e) {}
		}
		protected void cleanup(OutputCollector<Text, DoubleWritable> output) throws IOException, InterruptedException {}
    }
    
    
    
    //Reducer
    public static class Reduce extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    	// Configure and setup
		public void configure(JobConf job) {}
		//protected void setup(OutputCollector<Text, DoubleWritable> output) throws IOException, InterruptedException {}
		public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
			double smiley = 0.0;
    	    output.collect(key, new DoubleWritable(smiley));
		}
		protected void cleanup(OutputCollector<Text, DoubleWritable> output) throws IOException, InterruptedException {}
    }

    public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), Exercise3.class);
		conf.setJobName("Exercise3");
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(DoubleWritable.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(DoubleWritable.class);
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		//conf.setInputFormat(TextInputFormat.class);
		//conf.setOutputFormat(TextOutputFormat.class);
	
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
	
		JobClient.runJob(conf);
		return 0;
    }

    public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Exercise3(), args);
		System.exit(res);
    }
}
