package com.prithvi.mapreduce.logprocessing.logwritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class LogWritable implements WritableComparable<LogWritable> {

	private Text originatingIP;
	private Text clientIdentity;
	private Text userId;
	private Text timeStamp;
	private Text requestType;
	private Text requestPage;
	private Text httpProtocolVersion;
	private IntWritable responseCode;
	private IntWritable responseSize;
	private Text referrer;
	private Text userAgent;

	public LogWritable() {
		this.originatingIP = new Text();
		this.clientIdentity = new Text();
		this.userId = new Text();
		this.timeStamp = new Text();
		this.requestType = new Text();
		this.requestPage = new Text();
		this.httpProtocolVersion = new Text();
		this.responseCode = new IntWritable();
		this.responseSize = new IntWritable();
		this.referrer = new Text();
		this.userAgent = new Text();
	}

	public LogWritable(Text originatingIP, Text clientIdentity, Text userId,
			Text timeStamp, Text requestType, Text requestPage,
			Text httpProtocolVersion, IntWritable responseCode,
			IntWritable responseSize, Text referrer, Text userAgent) {
		this.originatingIP = originatingIP;
		this.clientIdentity = clientIdentity;
		this.userId = userId;
		this.timeStamp = timeStamp;
		this.requestType = requestType;
		this.requestPage = requestPage;
		this.httpProtocolVersion = httpProtocolVersion;
		this.responseCode = responseCode;
		this.responseSize = responseSize;
		this.referrer = referrer;
		this.userAgent = userAgent;
	}

	public Text getOriginatingIP() {
		return originatingIP;
	}

	public void setOriginatingIP(Text originatingIP) {
		this.originatingIP = originatingIP;
	}

	public Text getClientIdentity() {
		return clientIdentity;
	}

	public void setClientIdentity(Text clientIdentity) {
		this.clientIdentity = clientIdentity;
	}

	public Text getUserId() {
		return userId;
	}

	public void setUserId(Text userId) {
		this.userId = userId;
	}

	public Text getTimeStamp() {
		return timeStamp;
	}

	public void setTimeStamp(Text timeStamp) {
		this.timeStamp = timeStamp;
	}

	public Text getRequestType() {
		return requestType;
	}

	public void setRequestType(Text requestType) {
		this.requestType = requestType;
	}

	public Text getRequestPage() {
		return requestPage;
	}

	public void setRequestPage(Text requestPage) {
		this.requestPage = requestPage;
	}

	public Text getHttpProtocolVersion() {
		return httpProtocolVersion;
	}

	public void setHttpProtocolVersion(Text httpProtocolVersion) {
		this.httpProtocolVersion = httpProtocolVersion;
	}

	public IntWritable getResponseCode() {
		return responseCode;
	}

	public void setResponseCode(IntWritable responseCode) {
		this.responseCode = responseCode;
	}

	public IntWritable getResponseSize() {
		return responseSize;
	}

	public void setResponseSize(IntWritable responseSize) {
		this.responseSize = responseSize;
	}

	public Text getReferrer() {
		return referrer;
	}

	public void setReferrer(Text referrer) {
		this.referrer = referrer;
	}

	public Text getUserAgent() {
		return userAgent;
	}

	public void setUserAgent(Text userAgent) {
		this.userAgent = userAgent;
	}

	public void set(String originatingIP, String clientIdentity, String userId,
			String timeStamp, String requestType, String requestPage,
			String httpProtocolVersion, int responseCode, int responseSize,
			String referrer, String userAgent) {
		this.originatingIP.set(originatingIP);
		this.clientIdentity.set(clientIdentity);
		this.userId.set(userId);
		this.timeStamp.set(timeStamp);
		this.requestType.set(requestType);
		this.requestPage.set(requestPage);
		this.httpProtocolVersion.set(httpProtocolVersion);
		this.responseCode.set(responseCode);
		this.responseSize.set(responseSize);
		this.referrer.set(referrer);
		this.userAgent.set(userAgent);
	}

	public void readFields(DataInput datainput) throws IOException {
		originatingIP.readFields(datainput);
		clientIdentity.readFields(datainput);
		userId.readFields(datainput);
		timeStamp.readFields(datainput);
		requestType.readFields(datainput);
		requestPage.readFields(datainput);
		httpProtocolVersion.readFields(datainput);
		responseCode.readFields(datainput);
		responseSize.readFields(datainput);
		referrer.readFields(datainput);
		userAgent.readFields(datainput);

	}

	public void write(DataOutput dataoutput) throws IOException {
		originatingIP.write(dataoutput);
		clientIdentity.write(dataoutput);
		userId.write(dataoutput);
		timeStamp.write(dataoutput);
		requestType.write(dataoutput);
		requestPage.write(dataoutput);
		httpProtocolVersion.write(dataoutput);
		responseCode.write(dataoutput);
		responseSize.write(dataoutput);
		referrer.write(dataoutput);
		userAgent.write(dataoutput);
	}

	public boolean equals(Object o) {
		LogWritable other = (LogWritable) o;
		if (other != null) {
			if (this.requestPage.toString().equalsIgnoreCase(
					other.getRequestPage().toString())
					&& this.timeStamp.toString().equalsIgnoreCase(
							other.getTimeStamp().toString())) {
				return true;
			}
		}
		return false;
	}

	public int hashCode() {
		final int prime = 11;
		int result = 1;

		result = prime
				* result
				+ ((this.originatingIP == null) ? 0 : this.originatingIP
						.hashCode());
		result = result
				+ prime
				* result
				+ ((this.requestPage == null) ? 0 : this.requestPage.hashCode());
		return result;
	}

	public String toString() {
		return this.originatingIP.toString() + " "
				+ this.clientIdentity.toString() + " " + this.userId.toString()
				+ " " + this.timeStamp.toString() + " "
				+ this.requestType.toString() + " "
				+ this.requestPage.toString() + " "
				+ this.httpProtocolVersion.toString() + " "
				+ this.responseCode.get() + " " + this.responseSize.get() + " "
				+ this.referrer.toString() + " " + this.userAgent.toString();
	}

	public int compareTo(LogWritable o) {
		LogWritable other = (LogWritable) o;
		if (equals(other)) {
			return 0;
		} else {
			if (!this.requestPage.toString().equalsIgnoreCase(
					other.getRequestPage().toString())) {
				return this.requestPage.toString().compareTo(
						other.getRequestPage().toString());
			} else {
				return this.timeStamp.toString().compareTo(
						other.getTimeStamp().toString());
			}
		}
	}

}
