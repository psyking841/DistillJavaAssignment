package com.distilljavaassignment.aggregator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class LogReducer {
	Map<String, Integer> domainCount; //domain and its requests count
	Map<String, Map<String, Integer>> domainHourCount; //domain : <hour : count>, domain and its request count in different hours
	Map<String, Integer> domainMax; //domain and its max count
	Map<String, Double> domainAverage; //domain and its average count across hours
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
