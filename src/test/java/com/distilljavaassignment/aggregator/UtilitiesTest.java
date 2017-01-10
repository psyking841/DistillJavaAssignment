package com.distilljavaassignment.aggregator;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

//import com.distilljavaassignment.aggregator.Record;
//import com.distilljavaassignment.aggregator.LogProducer;

public class UtilitiesTest {

//	@Test
//	public void testReadFile(){
//		File infile = new File("/Users/SPan/Downloads/Data-Engineer-Exercise-Data/distil-exercise-01.tsv");
//		String tmpDir = "/Users/SPan/Downloads/tmp/";
//		
//		//create tmp folder if it does not exist
//		File tmpDirFile = new File(tmpDir);
//		if (!tmpDirFile.exists()) {//check if folder exist
//			System.out.println("temp dir does not exist");
//			if (!tmpDirFile.mkdirs()) {//check if success
//				System.out.println("Temp dir creation failed！");
//				return;
//			}
//		}
//		
//		LogWorker worker = new LogWorker(infile, tmpDir);
//		Map<String, Map<String, Integer>> hourDomainCount = new HashMap<>();
//		worker.readLogFile(infile, hourDomainCount);
//		assertEquals(13, hourDomainCount.size());
//		
//		String tmpFileName = tmpDir + infile.getName() + ".tmp";
//		File tmpFile = new File(tmpFileName);
//		worker.writeResult(tmpFile, hourDomainCount);
//	}
	
	@Test
	public void testDeleteDir(){
		String tmpDir = "/Users/SPan/Downloads/tmp/";
		LogReducer reducer = new LogReducer(tmpDir);
		reducer.deleteDirectory(tmpDir);
	}
}
