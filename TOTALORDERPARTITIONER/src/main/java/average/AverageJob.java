package average;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class AverageJob extends Configured implements Tool {
	
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
		
		Configuration conf = getConf();
		Job job = Job.getInstance(conf);
		job.setJarByClass(getClass());
		
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setMapperClass(AverageMapper.class);
		job.setCombinerClass(AverageCombiner.class);
		
		job.setPartitionerClass(TotalOrderPartitioner.class);
		
		job.setReducerClass(AverageReducer.class);
		
		job.setNumReduceTasks(6);
		
		InputSampler.Sampler<Text, Text> sampler = new InputSampler.RandomSampler<Text, Text>(0.1, 200, 3);
		InputSampler.writePartitionFile(job, sampler);
		
		String partitionFile = TotalOrderPartitioner.getPartitionFile(conf);
		URI partitionUri = new URI(partitionFile + "#" + TotalOrderPartitioner.DEFAULT_PATH);
		job.addCacheFile(partitionUri);

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