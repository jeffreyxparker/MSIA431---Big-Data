//package mr_app.Homework2;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.*;

public class Exercise1 extends Configured implements Tool {
	
	// One Gram Mapper
    public static class FirstMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {
    	// Configure and setup
		public void configure(JobConf job) {}
		protected void setup(OutputCollector<Text, DoubleWritable> output) throws IOException, InterruptedException {}
		public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
	    	try {
				String text = value.toString();
				int year= Integer.parseInt(text.split("\\s+")[1]);
				String gram = text.split("\\s+")[0];
			    //int num_occurances= Integer.parseInt(text.split("\\s+")[2]);
				int num_volumes= Integer.parseInt(text.split("\\s+")[3]);
	
				if(gram.toLowerCase().contains("nu")) {
			    	String year_substring =  Integer.toString(year) + ",nu";
			        output.collect(new Text(year_substring),new DoubleWritable(num_volumes));
			    	//System.out.println("("+year_substring+","+num_volumes+")");
		    	}
				if(gram.toLowerCase().contains("chi")) {
			    	String year_substring =  Integer.toString(year) + ",chi";
			        output.collect(new Text(year_substring),new DoubleWritable(num_volumes));
			    	//System.out.println("("+year_substring+","+num_volumes+")");
			    	}
				if(gram.toLowerCase().contains("haw")) {
			    	String year_substring =  Integer.toString(year) + ",haw";
			        output.collect(new Text(year_substring),new DoubleWritable(num_volumes));
			    	//System.out.println("("+year_substring+","+num_volumes+")");
			    	}
			}
			catch (Exception e) {}
		}
		protected void cleanup(OutputCollector<Text, DoubleWritable> output) throws IOException, InterruptedException {}
    }
    
	// Two Gram Mapper
    public static class SecondMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {
    	// Configure and setup
		public void configure(JobConf job) {}
		protected void setup(OutputCollector<Text, DoubleWritable> output) throws IOException, InterruptedException {}
		public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
			try {
				String text = value.toString();
				int year= Integer.parseInt(text.split("\\s+")[2]);
				String gram1 = text.split("\\s+")[0];
				String gram2 = text.split("\\s+")[1];
				String gram = gram1 +" "+ gram2;
			    //int num_occurances= Integer.parseInt(text.split("\\s+")[3]);
				int num_volumes= Integer.parseInt(text.split("\\s+")[4]);

				if(gram.toLowerCase().contains("nu")) {
			    	String year_substring =  Integer.toString(year) + ",nu";
			        output.collect(new Text(year_substring),new DoubleWritable(num_volumes));
			    	//System.out.println("("+year_substring+","+num_volumes+")");
		    	}
				if(gram.toLowerCase().contains("chi")) {
			    	String year_substring =  Integer.toString(year) + ",chi";
			        output.collect(new Text(year_substring),new DoubleWritable(num_volumes));
			    	//System.out.println("("+year_substring+","+num_volumes+")");
			    	}
				if(gram.toLowerCase().contains("haw")) {
			    	String year_substring =  Integer.toString(year) + ",haw";
			        output.collect(new Text(year_substring),new DoubleWritable(num_volumes));
			    	//System.out.println("("+year_substring+","+num_volumes+")");
			    	}
			}
			catch (Exception e) {}
		}
		protected void cleanup(OutputCollector<Text, DoubleWritable> output) throws IOException, InterruptedException {}
    }
    
    
    

    public static class Reduce extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		public void configure(JobConf job) {}
		protected void setup(OutputCollector<Text, DoubleWritable> output) throws IOException, InterruptedException {}
	
		public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
    	    
        	double ave= 0.0;
    	    double sum = 0.0;
    	    int count =0;
    	    while (values.hasNext()) {
    	    double cur = values.next().get(); 
    	    count ++;
    	    sum += cur;
    	    }
    	    ave = sum / (double) count;
    	    output.collect(key, new DoubleWritable(ave));
    	    
		}
		protected void cleanup(OutputCollector<Text, DoubleWritable> output) throws IOException, InterruptedException {}
    }

    public int run(String[] args) throws Exception {
	JobConf conf = new JobConf(getConf(), Exercise1.class);
	conf.setJobName("Exercise1");
	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(DoubleWritable.class);
	//conf.setMapperClass(Map.class);
	conf.setCombinerClass(Reduce.class);
	conf.setReducerClass(Reduce.class);
	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);

    MultipleInputs.addInputPath(conf, new Path(args[0]), TextInputFormat.class, FirstMap.class);
    MultipleInputs.addInputPath(conf, new Path(args[1]), TextInputFormat.class, SecondMap.class);
	FileOutputFormat.setOutputPath(conf, new Path(args[2]));

	JobClient.runJob(conf);
	return 0;
    }

    public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Configuration(), new Exercise1(), args);
	System.exit(res);
    }
}
