package com.distilljavaassignment.aggregator;

import sun.misc.Signal;
import sun.misc.SignalHandler;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

class IsCloseObj {
	public boolean isClose = false;
}

public class LogAggregator {
	final static ExecutorService THREAD_POOL = Executors.newFixedThreadPool(3); //three threads
	private static final IsCloseObj closeFlag = new IsCloseObj();
	
	public static void main(String[] args){
		//String logDir ="/Users/SPan/Downloads/Data-Engineer-Exercise-Data/";				
		//String tmpDir ="/Users/SPan/Downloads/tmp/";
		String logDir = args[1];
		String tmpDir = args[2];
		
		LogProcessor processor = new LogProcessor(THREAD_POOL, logDir, tmpDir, closeFlag);
		
		processor.rollup();
	}
	
	//put init code in static block
	//this code register a signal handler for SIGTERM
	static{
		Signal sig = new Signal("TERM");
		Signal.handle(sig, new SignalHandler() {
			@Override
			public void handle(Signal signal) {
				System.out.println("Got kill signal, about to exit");
				closeFlag.isClose = true;
				THREAD_POOL.shutdown();
				while (!THREAD_POOL.isTerminated()) {
                    try {
                        //check every 200 ms
                        TimeUnit.MILLISECONDS.sleep(200L);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                System.out.println("Threads in the thread pool have all completed!");
                System.exit(0);
            }
        });
	}
}
