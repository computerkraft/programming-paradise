package custominpfmt;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.StringUtils;

public class StockInputFormat extends FileInputFormat<Stocks, StockPrices> {

	public static class StockReader extends RecordReader<Stocks, StockPrices> {
		
		private Stocks key = new Stocks();
		private StockPrices value = new StockPrices();
		private BufferedReader in;

		@Override
		public void close() throws IOException {
			in.close();
		}

		@Override
		public Stocks getCurrentKey() throws IOException, InterruptedException {

				return key;
		}

		@Override
		public StockPrices getCurrentValue() throws IOException,
				InterruptedException {

			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {

			return 0;
		}

		@Override
		public void initialize(InputSplit inputSplit, TaskAttemptContext context)
				throws IOException, InterruptedException {
			
			FileSplit split = (FileSplit) inputSplit;
			
			Configuration conf = context.getConfiguration();
			Path path = split.getPath();
			InputStream is = path.getFileSystem(conf).open(path);
			
			in= new BufferedReader(new InputStreamReader(is));

		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {

			String line = in.readLine();
			if (line == null) return false;
			
			String[] words = StringUtils.split(line, '\\', ',');
			if (words[0].trim().equals("exchange")){
				line = in.readLine();
				words = StringUtils.split(line, '\\', ',');
			}
			
			key.setSymbol(words[1]);
			key.setDate(words[2]);
			
			value.setOpen(Double.parseDouble(words[3]));
			value.setHigh(Double.parseDouble(words[4]));
			value.setLow(Double.parseDouble(words[5]));
			value.setClose(Double.parseDouble(words[6]));
			value.setVolume(Integer.parseInt(words[7]));
			value.setAdjclose(Double.parseDouble(words[8]));
			
			return true;
	
		}	

	}

	@Override
	public RecordReader<Stocks, StockPrices> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException, InterruptedException {

		return new StockReader();
		
	}

}