package customopfmt;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DividendJob extends Configured implements Tool {
	
		

	public static class DividendChange implements Writable {
		
		private String symbol;
		private String date;
		private Double change;
		
		@Override
		public void write(DataOutput out) throws IOException {
			
			out.writeUTF(symbol);
			out.writeUTF(date);
			out.writeDouble(change);
			
		}

		@Override
		public void readFields(DataInput in) throws IOException {
				
			symbol= in.readUTF();
			date= in.readUTF();
			change= in.readDouble();

		}

		public String getSymbol() {
			return symbol;
		}

		public void setSymbol(String symbol) {
			this.symbol = symbol;
		}

		public String getDate() {
			return date;
		}

		public void setDate(String date) {
			this.date = date;
		}

		public Double getChange() {
			return change;
		}

		public void setChange(Double change) {
			this.change = change;
		}

		@Override
		public String toString() {

			return (symbol + " - " + date + " - " + change);
		}

	}


	public static class CustomKey implements WritableComparable<CustomKey> {
		
		private String symbol;
		private String date;

		@Override
		public void write(DataOutput out) throws IOException {
				
			out.writeUTF(symbol);
			out.writeUTF(date);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			
			symbol = in.readUTF();
			date = in.readUTF();
			
		}

		public int compareTo(CustomKey obj) {

			int response = this.symbol.compareTo(obj.symbol);
			
			if (response == 0)
				response = this.date.compareTo(obj.date);
			
			return response;
		}

		public String getSymbol() {
			return symbol;
		}

		public void setSymbol(String symbol) {
			this.symbol = symbol;
		}

		public String getDate() {
			return date;
		}

		public void setDate(String date) {
			this.date = date;
		}

	}
	
	
	public static class CustomComparator extends WritableComparator {
		
		
		protected CustomComparator(){
			
			super(CustomKey.class,true);
			
		}
		
		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			
			CustomKey lhs = (CustomKey) a;
			CustomKey rhs = (CustomKey) b;
			return lhs.getSymbol().compareTo(rhs.getSymbol());
			
		}

	}


	public static class DividendMapper extends Mapper<LongWritable, Text, CustomKey, DoubleWritable> {
		
		private CustomKey mapOutputKey = new CustomKey();
		private DoubleWritable mapOutputValue = new DoubleWritable();
		private final String EXCHANGE = "exchange";
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
			
			String[] word = StringUtils.split(value.toString(), '\\', ',');
			if (EXCHANGE.equals(word[0])){
				return ;
			}
			
			mapOutputKey.setSymbol(word[1].trim());
			mapOutputKey.setDate(word[2].trim());
			
			mapOutputValue.set(Double.parseDouble(word[3].trim()));
			context.write(mapOutputKey, mapOutputValue);
			context.getCounter(Counters.MAP).increment(1);
		}

	}
	
	public static class DividendReducer extends Reducer<CustomKey, DoubleWritable, NullWritable, DividendChange> {

		private NullWritable reduceOutputKey = NullWritable.get();
		private DividendChange reduceOutputValue = new DividendChange();
		
		protected void reduce(CustomKey key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {
			
			double prevDividend=0.0;
			for(DoubleWritable value: values)
			{
				double curDividend = value.get();
				double growth = curDividend - prevDividend;
				if (Math.abs(growth) > 0.000001){
					
					prevDividend = curDividend;
					reduceOutputValue.setSymbol(key.getSymbol());
					reduceOutputValue.setDate(key.getDate());
					reduceOutputValue.setChange(growth);
					context.write(reduceOutputKey, reduceOutputValue);
					
				}
			}
			context.getCounter(Counters.REDUCE).increment(1);
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
		
		job.setMapperClass(DividendMapper.class);
		job.setReducerClass(DividendReducer.class);
		job.setGroupingComparatorClass(CustomComparator.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(DividendOutputFormat.class);
		
		job.setMapOutputKeyClass(CustomKey.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(DividendChange.class);
		
		job.setNumReduceTasks(3);

		return job.waitForCompletion(true)?0:1;
	}

	public static void main(String[] args) {
		
		int result=0;
		try
		{
			result = ToolRunner.run(new Configuration(), new DividendJob(), args);
			System.exit(result);
		}
		catch (Exception e)
		{
			e.printStackTrace();			
		}
	}
}