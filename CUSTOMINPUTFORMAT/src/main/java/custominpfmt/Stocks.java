package custominpfmt;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

class Stocks implements WritableComparable<Stocks> {
	
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

	public int compareTo(Stocks obj) {

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

	@Override
	public String toString() {

		return symbol + "\t" + date;
	}

}