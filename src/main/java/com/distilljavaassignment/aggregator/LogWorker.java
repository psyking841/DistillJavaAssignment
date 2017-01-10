package com.distilljavaassignment.aggregator;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class LogWorker implements Runnable{
	final String tmpDir;
	final File logFile;
	final File tmpFile; //corresponding results file
	Map<String, Map<String, Integer>> hourDomainCount; //date : <domain : requestCount>
	
	public LogWorker(File logFile, String tmpDir){
		hourDomainCount = new HashMap<>();
		this.logFile = logFile;
		this.tmpDir = tmpDir;
		String tmpFileName = tmpDir + logFile.getName() + ".tmp";
		this.tmpFile = new File(tmpFileName);
	}
	
	public void run(){
		//first check if results exist
		if(tmpFile.exists()){
			return;
		}
		//else
		//1. process log file
		readLogFile(logFile, hourDomainCount);
		//2. write results map to intermediate tmp file;
		writeResult(tmpFile, hourDomainCount);
		
	}
	
	public void readLogFile(File f, Map<String, Map<String, Integer>> resMap){		
		BufferedReader reader = null;
		try {
            reader = new BufferedReader(new FileReader(logFile));  
            String lineString = null;
            int line = 1;  
            // read until we hit null  
            while ((lineString = reader.readLine()) != null) {  
                // display line number 
                System.out.println("line " + line + ": " + lineString);  
                line++;
                //parse line and read it to the queue
                String[] str = lineString.split("\t");
                double timestamp = Double.parseDouble(str[0]) * 1000;
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH");
                String timeStr = sdf.format(new Date((long) timestamp));
                
                String domain = str[1];
                //String request = str[2];
                
                if(!resMap.containsKey(timeStr)){
                	resMap.put(timeStr, new HashMap<String, Integer>());
                }
                
                Map<String, Integer> domainCount = resMap.get(timeStr);
                
                if(!domainCount.containsKey(domain)){
                	domainCount.put(domain, 0);
                }
                
                domainCount.put(domain, domainCount.get(domain)+1);
                Thread.sleep(100);
            }
           
            reader.close();            
        } catch (IOException e) {  
            e.printStackTrace();  
        } catch (InterruptedException ie) {
			ie.printStackTrace();
		}		
	}
	
	public void writeResult(File file, Map<String, Map<String, Integer>> resMap){
		BufferedWriter bw = null;
		try {
			//create file
			file.createNewFile();
			
			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			bw = new BufferedWriter(fw);
			//start writing to file
			for(Map.Entry<String, Map<String, Integer>> entry : resMap.entrySet()){
				String domain = entry.getKey();
				for(Map.Entry<String, Integer> subEntry : resMap.get(domain).entrySet()){
					String content = domain + "\t" + subEntry.getKey() + "\t" + subEntry.getValue() + '\n';
					try {
						bw.write(content);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		} finally {
			try {
				if(bw!=null){
					bw.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		System.out.println("Mapper " + Thread.currentThread().getName() + " Done");
	}
}
