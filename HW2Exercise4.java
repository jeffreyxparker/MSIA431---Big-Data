//package mr_app.Homework2;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.*;

public class Exercise4 extends Configured implements Tool {
	
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
				double duration = Double.parseDouble(line.split(",")[3]);
				String tuple = title+","+artist+","+duration+","+year;
				double smiley = 0.0;

				if(year >= 2000 && year <= 2010) {
					output.collect(new Text(artist), new DoubleWritable(duration));
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
			double max = 0.0;
			while (values.hasNext()) {
				double cur = values.next().get();
		    	if (cur > max) {
		    		max = cur;
		    	}
		    }
    	    output.collect(key, new DoubleWritable(max));
		}
		protected void cleanup(OutputCollector<Text, DoubleWritable> output) throws IOException, InterruptedException {}
    }
    
    
	//Partitioner
	public static class Partition extends MapReduceBase implements Partitioner<Text, DoubleWritable> {
	    public int getPartition(Text key, DoubleWritable value, int numPartitions) {
	        if (key.toString().charAt(0) >= 'A' && key.toString().charAt(0) <= 'F') {
	            return 0;
	        } else if (key.toString().charAt(0) >= 'G' && key.toString().charAt(0) <= 'K') {
	            return 1;
	        } else if (key.toString().charAt(0) >= 'L' && key.toString().charAt(0) <= 'P') {
	            return 2;
	        } else if (key.toString().charAt(0) >= 'Q' && key.toString().charAt(0) <= 'U') {
	            return 3;
	        } else if (key.toString().charAt(0) >= 'V' && key.toString().charAt(0) <= 'Z'){
	            return 4;
	        } else {
	        	return 5;
	        }
	    }
	}

    public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), Exercise4.class);
		conf.setJobName("Exercise4");
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(DoubleWritable.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(DoubleWritable.class);
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		//conf.setInputFormat(TextInputFormat.class);
		//conf.setOutputFormat(TextOutputFormat.class);
		conf.setPartitionerClass(Partition.class);
	
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
	
		JobClient.runJob(conf);
		return 0;
    }

    public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Exercise4(), args);
		System.exit(res);
    }
}
