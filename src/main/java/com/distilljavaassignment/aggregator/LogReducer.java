package com.distilljavaassignment.aggregator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class LogReducer {
	Map<String, Integer> domainCount;
	Map<String, Map<String, Integer>> domainHourCount; //domain : <hour : count>
	Map<String, Integer> domainMax;
	Map<String, Double> domainAverage;
	String tmpDir;
	File[] resultFiles;
	
	public LogReducer(String tmpDir){
		this.tmpDir = tmpDir;
		this.resultFiles = new File(tmpDir).listFiles(new FileFilter(){
			@Override
			public boolean accept(File file){
				return file.isFile() && file.getName().endsWith(".tmp");
			}
		});
		this.domainCount = new HashMap<>();
		this.domainHourCount = new HashMap<>();
		this.domainMax = new HashMap<>();
		this.domainAverage = new HashMap<>();
	}
	
	public void reduce(){
		//read all results files and update maps above
		for(File f : resultFiles){
			readResultFile(f);
		}
		
		//post processing to get hourly average
		for(Map.Entry<String, Map<String, Integer>> entry : domainHourCount.entrySet()){
			String domain = entry.getKey();
			int numCount = domainCount.get(domain);
			int numHours = entry.getValue().size();
			double avg = (numCount*1.0)/numHours;
			
			if(!domainAverage.containsKey(domain)){
				domainAverage.put(domain, 0.0);
			}
			domainAverage.put(domain, avg);
		}
		
		System.out.println("Reducer Done!");
		System.out.println("Clean up results files!");
		deleteDirectory(tmpDir);
	}
	
	public void deleteFile(String filePath) {
		File file = new File(filePath);
		if (file.isFile() && file.exists()) {
			file.delete();
		}
	}
	
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
	
	private void readResultFile(File f){		
		BufferedReader reader = null;
		try {
            reader = new BufferedReader(new FileReader(f));  
            String lineString = null;
            int line = 1;  
            // read until we hit null  
            while ((lineString = reader.readLine()) != null) {  
                // display line number 
                System.out.println("line " + line + ": " + lineString);  
                line++;
                //parse line and read it to the queue
                String[] str = lineString.split("\t");
                String timestamp = str[0];
                String domain = str[1];
                String count = str[2];
                
                //update domain count
                if(!domainCount.containsKey(domain)){
                	domainCount.put(domain, 0);
                }
                
                domainCount.put(domain, domainCount.get(domain)+1);
                
                //update domain max
                if(!domainMax.containsKey(domain)){
                	domainMax.put(domain, Integer.MIN_VALUE);
                }
                
                domainMax.put(domain, Math.max(domainMax.get(domain), Integer.parseInt(count)));
                
                //update domain hour count
                if(!domainHourCount.containsKey(domain)){
                	domainHourCount.put(domain, new HashMap<>());
                }
                
                Map<String, Integer> hourCount = domainHourCount.get(domain);
                if(!hourCount.containsKey(timestamp)){
                	hourCount.put(timestamp, 0);
                }
                hourCount.put(timestamp, hourCount.get(timestamp) + 1);
                
            }            
        } catch (IOException e) {  
            e.printStackTrace();  
        } finally {
        	try {
        		if(reader != null){
        			reader.close();
        		}
			} catch (IOException e) {
				e.printStackTrace();
			}
        }
	}
}
