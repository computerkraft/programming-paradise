package inverted;

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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class IndexInvertedJob extends Configured implements Tool {


	public static class IndexInverterMapper extends Mapper<LongWritable, Text, Text, Text> {

		private Text mapOutputKey = new Text();
		private Text mapOutputValue = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String[] words = StringUtils.split(value.toString(), '\\', ',');
			mapOutputValue.set(words[0].toString());
			for(int i=1; i < words.length; i++)
			{
				mapOutputKey.set(words[i].toString());
				context.write(mapOutputKey, mapOutputValue);
			}
		}
	}

	public static class IndexInverterReducer extends Reducer<Text, Text, Text, Text> {

		private Text reduceOuputValue = new Text();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			StringBuilder outputValue = new StringBuilder();
			for(Text value: values)
			{
				outputValue.append(value.toString()).append(",");
			}
			
			outputValue.deleteCharAt(outputValue.length() -1 );
			
			reduceOuputValue.set(outputValue.toString());
			context.write(key, reduceOuputValue);
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
		
		job.setMapperClass(IndexInverterMapper.class);
		job.setReducerClass(IndexInverterReducer.class);
		
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		return job.waitForCompletion(true)?0:1;
	}

	public static void main(String[] args) {
		
		int result=0;
		try
		{
			result = ToolRunner.run(new Configuration(), new IndexInvertedJob(), args);
			System.exit(result);
		}
		catch (Exception e)
		{
			e.printStackTrace();			
		}
	}
}