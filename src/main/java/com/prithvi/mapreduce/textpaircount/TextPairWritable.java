package com.prithvi.mapreduce.textpaircount;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class TextPairWritable implements WritableComparable<TextPairWritable> {
	public String getFirstString() {
		return firstString;
	}

	public void setFirstString(String firstString) {
		this.firstString = firstString;
	}

	public String getSecondString() {
		return secondString;
	}

	public void setSecondString(String secondString) {
		this.secondString = secondString;
	}

	private String firstString;
	private String secondString;

	public TextPairWritable() {
		this.firstString = null;
		this.secondString = null;
	}

	public TextPairWritable(String firstString, String secondString) {
		this.firstString = firstString;
		this.secondString = secondString;
	}

	public void readFields(DataInput datainput) throws IOException {
		// Following steps if instance variables are declared as Text instead of String
		// firstString.readFields(datainput);
		// secondString.readFields(datainput);
		firstString = datainput.readUTF();
		secondString = datainput.readUTF();
	}

	public void write(DataOutput dataoutput) throws IOException {
		dataoutput.writeUTF(firstString);
		dataoutput.writeUTF(secondString);
	}

	public boolean equals(TextPairWritable o) {
		TextPairWritable other = (TextPairWritable) o;
		if (this.firstString.equalsIgnoreCase(other.getFirstString())
				&& this.secondString.equalsIgnoreCase(other.getSecondString())) {
			return true;
		}
		return false;
	}

	public int compareTo(TextPairWritable o) {

		TextPairWritable other = (TextPairWritable) o;
		if (equals(other)) {
			return 0;
		} else {
			if (!this.firstString.equalsIgnoreCase(other.getFirstString())) {
				return this.firstString.compareTo(other.getFirstString());
			} else {
				return (this.secondString.compareTo(other.getSecondString()));
			}
		}
	}
}
