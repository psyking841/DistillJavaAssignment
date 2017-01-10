package com.distilljavaassignment.aggregator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.distilljavaassignment.aggregator.LogWorker;

public class UtilitiesTest {

	@Test
	public void testReadFile(){
		File infile = new File("src/test/resources/logs/distil-exercise-01.tsv");
		String tmpDir = "src/test/resources/tmp/";
		
		//create tmp folder if it does not exist
		File tmpDirFile = new File(tmpDir);
		if (!tmpDirFile.exists()) {//check if folder exist
			System.out.println("temp dir does not exist");
			if (!tmpDirFile.mkdirs()) {//check if success
				System.out.println("Temp dir creation failed！");
				return;
			}
		}
		
		LogWorker worker = new LogWorker(infile, tmpDir, new IsCloseObj());
		Map<String, Map<String, Integer>> hourDomainCount = new HashMap<>();
		worker.readLogFile(infile, hourDomainCount);
		assertEquals(13, hourDomainCount.size());
		
		String tmpFileName = tmpDir + infile.getName() + ".tmp";
		File tmpFile = new File(tmpFileName);
		worker.writeResult(tmpFile, hourDomainCount);
		assertTrue(tmpFile.exists());
	}
	
	@Test
	public void testDeleteDir(){
		String tmpDir = "src/test/resources/tmp/";
		LogReducer reducer = new LogReducer(tmpDir);
		reducer.deleteDirectory(tmpDir);
		assertFalse(new File(tmpDir).exists());
	}
}
