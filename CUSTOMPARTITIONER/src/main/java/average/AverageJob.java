package average;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class AverageJob extends Configured implements Tool {
	
	public static class AverageCustomPartitioner extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numberReduceTasks) {
			
			StringBuilder sb = new StringBuilder(key.toString());
			sb.deleteCharAt(1);
			return((sb.toString().hashCode() & Integer.MAX_VALUE) % numberReduceTasks);
			
		}

	}

	public enum Counters {MAP, COMINE, REDUCE};

	public static class AverageMapper extends Mapper<LongWritable, Text, Text, Text> {

		private Text mapOutputKey = new Text();
		private Text mapOutputValue = new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String[] words = StringUtils.split(value.toString(), '\\', ',');
			mapOutputKey.set(words[1].trim());
	
			StringBuilder moValue = new StringBuilder();
			moValue.append(words[9].trim()).append(",1");
			mapOutputValue.set(moValue.toString());
			context.write(mapOutputKey, mapOutputValue);
			
			context.getCounter(Counters.MAP).increment(1);
		}
	}
	
	public static class AverageCombiner extends Reducer<Text, Text, Text, Text> {

		private Text combinerOutputValue = new Text();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			int count=0;
			long sum=0;
			for(Text value: values)
			{
				String[] strValues = StringUtils.split(value.toString(), ','); 
				sum+= Long.parseLong(strValues[0]);
				count+= Integer.parseInt(strValues[1]);
			}
			combinerOutputValue.set(sum + "," + count);
			context.write(key, combinerOutputValue);
			
			context.getCounter(Counters.COMINE).increment(1);
		}
	}
	

	public static class AverageReducer extends Reducer<Text, Text, Text, DoubleWritable> {

		
		private DoubleWritable reduceOutputKey = new DoubleWritable();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			int count=0;
			double sum=0;
			for(Text value: values)
			{
				String[] strValues = StringUtils.split(value.toString(), ',');
				sum+= Double.parseDouble(strValues[0]);
				count+= Integer.parseInt(strValues[1]);
			}
			
			reduceOutputKey.set(sum/count);
			context.write(key, reduceOutputKey);
			
			context.getCounter(Counters.REDUCE).increment(1);
		}

	}


	@Override
	public int run(String[] args) throws Exception {
		
		Job job = Job.getInstance(getConf());
		job.setJarByClass(getClass());
		
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		
		job.setMapperClass(AverageMapper.class);
		job.setCombinerClass(AverageCombiner.class);
		job.setNumReduceTasks(6);
		job.setPartitionerClass(AverageCustomPartitioner.class);
		job.setReducerClass(AverageReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		return job.waitForCompletion(true)?0:1;
	}

	public static void main(String[] args) {
		
		int result=0;
		try
		{
			result = ToolRunner.run(new Configuration(), new AverageJob(), args);
			System.exit(result);
		}
		catch (Exception e)
		{
			e.printStackTrace();			
		}
	}
}