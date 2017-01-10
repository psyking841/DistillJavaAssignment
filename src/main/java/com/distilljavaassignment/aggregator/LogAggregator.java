package com.distilljavaassignment.aggregator;

public class LogAggregator {
	public static void main(String[] args){
		String logDir ="/Users/SPan/Downloads/Data-Engineer-Exercise-Data/";				
		String tmpDir ="/Users/SPan/Downloads/tmp/";
		//could use tmp dir
		//System.getProperty("java.io.tmpdir") + "/";
		int numThreads = 3;
		LogProcessor processor = new LogProcessor(logDir, tmpDir, numThreads);
		
		processor.rollup();
	}
}
