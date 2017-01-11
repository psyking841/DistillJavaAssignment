package com.distilljavaassignment.aggregator;

import java.io.File;
import java.io.FileFilter;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;


public class LogProcessor {
	final ExecutorService executor;
	String fileDir;
	final File[] logFiles;
	final String tmpDir;
	IsCloseObj closeFlag;
	
	public LogProcessor(ExecutorService executor, String dir, String tmpDir, IsCloseObj closeFlag){
		this.executor = executor;
		this.closeFlag = closeFlag;
		//add separator if it is not there
		if (tmpDir!=null && !tmpDir.endsWith(File.separator)) {
			tmpDir = tmpDir + File.separator;
		}
		
		if (dir!=null && !dir.endsWith(File.separator)) {
			dir = dir + File.separator;
		}
		
		this.logFiles = getFiles(dir); 
		this.tmpDir = tmpDir;
	}
	
	/**
	 * Get all files under a directory
	 * @param dir
	 * @return
	 */
	public File[] getFiles(String dir){
		if(dir == null){
			return null;
		}
		
		final File seedsDir = new File(dir);
		//we only want .tsv file
		return seedsDir.listFiles(new FileFilter(){
			@Override
			public boolean accept(File file){
				return file.isFile() && file.getName().endsWith(".tsv");
			}
		});
	}
	
	/**
	 * The main workflow is here...
	 */
	public void rollup(){
		//do nothing if there is no log file
		if(logFiles == null || logFiles.length == 0){
			return;
		}
		
		//create tmp folder if it does not exist
		File tmpFile = new File(tmpDir);
		if (!tmpFile.exists()) {//check if folder exist
			System.out.println("temp dir does not exist, will create it");
			if (!tmpFile.mkdirs()) {//check if success
				System.out.println("Temp dir creation failed！");
				return;
			}
		}
		
		//workers will check if corresponding results files exist
		//if so it will do nothing
		for(final File logFile : logFiles){
			LogWorker worker = new LogWorker(logFile, tmpDir, closeFlag);
			executor.execute(worker);
		}
		
		try{
			executor.shutdown();
			executor.awaitTermination(1, TimeUnit.HOURS);
		}catch(InterruptedException ie){
			System.out.println(ie);
		}finally {
			executor.shutdownNow();
			System.out.println("Shutdown thread poll");
			if(closeFlag.isClose){
				System.out.println("Shutdown process");
				return;
			}
		}
		
		//once finished, we reduce the results in the results files
		LogReducer reducer = new LogReducer(tmpDir);
		reducer.reduce();
		
		System.out.println("Clean up results files!");
		deleteDirectory(tmpDir);
		
		//output results to stdout
		System.out.println("Metrics Summary:");
		for(Map.Entry<String, Integer> entry : reducer.domainCount.entrySet()){
			String domain = entry.getKey();
			System.out.println("Total number of requests for domain: " + domain + " is " + reducer.domainCount.get(domain));
			System.out.println("Hourly average number of requests for domain: " + domain + " is " + reducer.domainAverage.get(domain));
			System.out.println("Maximum number of requests for domain: " + domain + " is " + reducer.domainMax.get(domain));
		}
	}
	
	/**
	 * Function to delete a file
	 * @param filePath
	 */
	public void deleteFile(String filePath) {
		File file = new File(filePath);
		if (file.isFile() && file.exists()) {
			file.delete();
		}
	}
	
	/**
	 * Function that recursively delete files in a folder and sub-folders
	 * @param dirPath
	 */
	public void deleteDirectory(String dirPath) {// delete dir and files under it
		File dirFile = new File(dirPath);
		
		if (!dirPath.endsWith(File.separator)) {
			dirPath = dirPath + File.separator;
		}
		
		if (!dirFile.exists() || !dirFile.isDirectory()) {
			return;
		}
		
		File[] files = dirFile.listFiles();// get all files
		for (int i = 0; i < files.length; i++) {
			if (files[i].isFile()) {
				deleteFile(files[i].getAbsolutePath());
				System.out.println(files[i].getAbsolutePath() + " has been deleted");
			} else {//recursively delete
				deleteDirectory(files[i].getAbsolutePath());
			}
		}
		
		dirFile.delete();
		System.out.println(dirFile.getAbsolutePath() + " has been deleted");
	}
}
