

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Exercise2 extends Configured implements Tool {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, FloatWritable> {

	public void configure(JobConf job) {
	}

	protected void setup(OutputCollector<Text, FloatWritable> output) throws IOException, InterruptedException {
	}

	public void map(LongWritable key, Text value, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {
	    String line = value.toString();
	    String[] linecsv = line.split(",");
	    int len= linecsv.length;
		String last_col= linecsv[len-1];
		//extracting columns 30 to 33
				int col30= (int)Double.parseDouble(linecsv[29]);
				int col31= (int)Double.parseDouble(linecsv[30]);
				int col32= (int)Double.parseDouble(linecsv[31]);
				int col33= (int)Double.parseDouble(linecsv[32]);
						
			    
		String tuple= Integer.toString(col30) +","+Integer.toString(col31)+ ","+Integer.toString(col32)+","+Integer.toString(col33)+",";		
	    float bivalue= Float.parseFloat(linecsv[3]);
	    if(last_col.equalsIgnoreCase("false")) {
            output.collect(new Text(tuple),new FloatWritable(bivalue));
	    }
	}

	protected void cleanup(OutputCollector<Text, FloatWritable> output) throws IOException, InterruptedException {
	}
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, FloatWritable, Text, FloatWritable> {

	public void configure(JobConf job) {
	}

	protected void setup(OutputCollector<Text, FloatWritable> output) throws IOException, InterruptedException {
	}

	public void reduce(Text key, Iterator<FloatWritable> values, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {
	    float ave= 0;
	    float sum = 0;
	    int count =0;
	    while (values.hasNext()) {
	    float cur = values.next().get(); 
	    count += 1;
	    sum += cur;
	    ave = sum / count;
	    }
	    output.collect(key, new FloatWritable(ave));
	}

	protected void cleanup(OutputCollector<Text, FloatWritable> output) throws IOException, InterruptedException {
	}
    }

    public int run(String[] args) throws Exception {
	JobConf conf = new JobConf(getConf(), Exercise2.class);
	conf.setJobName("Exercise2");

	// conf.setNumReduceTasks(0);

	// conf.setBoolean("mapred.output.compress", true);
	// conf.setBoolean("mapred.compress.map.output", true);

	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(FloatWritable.class);

	conf.setMapperClass(Map.class);
	conf.setCombinerClass(Reduce.class);
	conf.setReducerClass(Reduce.class);

	conf.setInputFormat(TextInputFormat.class); //non there
	conf.setOutputFormat(TextOutputFormat.class); //not there

	FileInputFormat.setInputPaths(conf, new Path(args[0]));
	FileOutputFormat.setOutputPath(conf, new Path(args[1]));

	JobClient.runJob(conf);
	return 0;
    }

    public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Configuration(), new Exercise2(), args);
	System.exit(res);
    }
}
