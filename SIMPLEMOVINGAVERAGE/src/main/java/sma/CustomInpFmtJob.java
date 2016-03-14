package sma;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CustomInpFmtJob extends Configured implements Tool {
	
		
	public static class CustomInpMapper extends Mapper<Stocks, StockPrices, Stocks, DoubleWritable> {
		
		private DoubleWritable mapOutputValue = new DoubleWritable();
				
		@Override
		protected void map(Stocks key, StockPrices value, Context context)
		throws IOException, InterruptedException {
			
			mapOutputValue.set(value.getClose());
			context.write(key, mapOutputValue);
			context.getCounter(Counters.MAP).increment(1);
		}

	}
	
	public static class CustomPartitioner extends Partitioner<Stocks, DoubleWritable> {

		@Override
		public int getPartition(Stocks key, DoubleWritable value, int numPartitions) {
			
			int partition = (key.getSymbol().charAt(0) -14) % numPartitions;
			return partition;

		}

	}

	
	public enum Counters {MAP, COMINE, REDUCE};

	@Override
	public int run(String[] args) throws Exception {
		
		Job job = Job.getInstance(getConf());
		job.setJarByClass(getClass());
		
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		
		job.setMapperClass(CustomInpMapper.class);
		job.setReducerClass(Reducer.class);
		job.setPartitionerClass(CustomPartitioner.class);
		
		job.setInputFormatClass(StockInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job.setMapOutputKeyClass(Stocks.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Stocks.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.setNumReduceTasks(1);

		return job.waitForCompletion(true)?0:1;
	}

	public static void main(String[] args) {
		
		int result=0;
		try
		{
			result = ToolRunner.run(new Configuration(), new CustomInpFmtJob(), args);
			System.exit(result);
		}
		catch (Exception e)
		{
			e.printStackTrace();			
		}
	}
}