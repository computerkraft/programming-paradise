package custominpfmt;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

class StockPrices implements Writable {
	
	private double open, high, low, close, adjclose;
	private int volume;

	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeDouble(open);
		out.writeDouble(high);
		out.writeDouble(low);
		out.writeDouble(close);
		out.writeDouble(adjclose);
		out.writeInt(volume);

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		
		open = in.readDouble();
		high = in.readDouble();
		low = in.readDouble();
		close = in.readDouble();
		adjclose = in.readDouble();	
		volume = in.readInt();

	}

	public double getOpen() {
		return open;
	}

	public void setOpen(double open) {
		this.open = open;
	}

	public double getLow() {
		return low;
	}

	public void setLow(double low) {
		this.low = low;
	}

	public double getClose() {
		return close;
	}

	public void setClose(double close) {
		this.close = close;
	}

	public double getAdjclose() {
		return adjclose;
	}

	public void setAdjclose(double adjclose) {
		this.adjclose = adjclose;
	}

	public int getVolume() {
		return volume;
	}

	public void setVolume(int volume) {
		this.volume = volume;
	}

	public double getHigh() {
		return high;
	}

	public void setHigh(double high) {
		this.high = high;
	}

}