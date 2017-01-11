This is a Java program that simulates Map/Reduce to achieve URL requests aggregation on the provided log files. 

The program consisits of 4 classes.
LogAggregator is the main class, which is the entrance to the process and provide SIGTERM handling. 
LogProcessor is the "Driver" class (it drives the mapping and reducing processes) for the whole aggregation process.
LogWorker is "mapper" class that aggregates the requests into intermediate results and stores them in a tmp file.
LogReducer combines the intermediate results from mappers and produces the final results.

Here is how the program meets the requirements:

1. A thread pool is created using ExecutorService interface from Java concurrent package. The pool size is set to 3 such that only 3 threads can be run concurrently. The thread pool pattern is more efficient here as tasks can run asynchronously. This is how this program meets requirement 1. 
2. Each thread runs a mapper which aggregates the requests in one log file. Because each thread is processing a independent log file, the program won't suffer from race condition. Each mapper processes one line at a time and two consecutive reads to the log file are performed over the 100 millisecond interval. This is how the program meets requirement 2.  
3. The intermediate results from a mapper are stored in a tmp file in local drive (the user should provide the path to the folder for storing these tmp files). The tmp file is named in such a way that the program can easily figure out which log files have already been processed by the mappers. As a result, in the case that the program needs to recover from a previous run, the program will skip processing log files have been processed and continue with those that have not. 
4. To smoothly close the program when receiving SIGTERM, sun.misc.signal package is used. A signal handler is created in a static block such that it will be initiated right before the program runs. Note that the THREAD_POOL variable (the implementation of ServiceExecutor Interface) is a public global variable, this allows the handler to access the pool when receiving the signal.
5. A global variable "isClose" (which is a object to a self-defined boolean class) is defined to pass "stop" signal to threads in the pool. Upon receiving the signal, this flag will be set to false such that incomplete threads in the pool will not start once receiving the SIGTERM signal. The running threads in the pool, however, will not be affected by this flag so they will continue the aggregation.
3, 4 and 5 together make the program meet requirement 3.
6. Once the mappers have done the aggregation, they will save all intermediate results into temp files. The "Driver" class will then start the reducer to aggregate the intermediate results in the temp files. The reducer will generate the final results for driver to display in stdout. This meets requirement 4.
7. At the end of the program, the driver will delete the tmp folder indicating the whole process has been finished.

The unittests are also provided for testing mappers and reducer's functionalities and file deletion.

Usage:

Step 1: build the project by command:

mvn package assembly:single

The jar file will be built under target folder

Step 2: run the jar file with command to use the program:

java -jar path-to-jar-file path-to-input-log-files-DIR path-to-tmp-files-DIR

Note,
The user has to provide the path to input log files and the path to tmp files (both are directories).
When recovering from previous aggregation, please use the same tmp files directory (the easiest way is to run the same command again). 

example command:
java -jar ./target/log-aggregator-0.0.1-SNAPSHOT-jar-with-dependencies.jar /Users/SPan/Downloads/Data-Engineer-Exercise-Data/ /Users/SPan/Downloads/tmp/