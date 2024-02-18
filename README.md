# ZBENCH

A test harness for performance testing and correctness validation of the delightful 
[Ziploq](https://github.com/manstegling/ziploq) data-merging Java library. 

This tool includes two command-line
applications _FileGenerator_ and _SyncApp_. The harness provides a streamlined sample implementation of a
Ziploq-based data processor sorting and merging _n_ sources online, in real-time. 

In addition to testing pruposes, it can be used as a reference for how to efficiently set up a Ziploq-based pipeline and
provide some interesting tricks when it comes to reading and writing newline-delimited json messages really, _really_ 
fast. For reference, on an AWS Ec2 c5d.2xlarge, it is producing and writing messages at ~1 million tps 
(single-threaded), while processing the data from, for example, 40 files simultaneously (reading, sorting and merging) 
is performed at an astonishing ~2.5 million tps (using 3 threads).

### FileGenerator

A command-line app for creating test data. Test data consists of deterministically random newline-delimited json files 
filled with `PerfMessage` messages. Message content is distributed with a decreasing number of messages per file, such 
that the first file has _m_ messages, the second has _m/2_ messages, the third _m/3_ messages, and so on. That means you
can always read a fixed number of messages from _k_ files resulting in _m_ messages in total, requiring a minimal disk
footprint. The data generator keeps track on what has already been generated through a file "filegen.meta", so that if 
you want to add more data later, it does not have to recreate already produced data.

The messages are partially ordered within each file, based on `timestamp` and `sequence`. Those values are not 
guaranteed to increase so there might be multiple messages having the same timestamp, same sequence, or both. This 
forces any user relying on total ordering to be careful when selecting tie-breakers and perhaps provide their own 
sorting.

Run `FileGenerator` by executing the following

```shell
java -cp zbench-2.0-SNAPSHOT.jar se.motility.zbench.generator.FileGenerator $NUM_FILES $DATA_PATH $MAX_MESSAGES
```

where 
* `NUM_FILES` is the number of files (data sources) to create, 
* `DATA_PATH` is the location to store the files, and 
* `MAX_MESSAGES` is the maximum number of messages available when reading the same number of messages from _k_ different
files, i.e. the highest possible `NUM_MESSAGES` value that will be available to SyncApp.

### SyncApp

A command-line app for benchmarking Ziploq simulating real-world usage. It can be used to compare performance between 
dedicated threads and different configurations of the managed Ziploq thread-pool. It outputs performance metrics as well
as checksums to verify implementations, e.g. for explorative analysis.

Run `SyncApp` by executing the following

```shell
java -XX:+UseG1GC -Xmx8G -Xlog:gc*,safepoint:gc.log -jar zbench-2.0-SNAPSHOT.jar $NUM_SOURCES $NUM_ITERATIONS $NUM_MESSAGES $DATA_PATH $POOLSIZE $BUFSIZE
```

where 
* `NUM_FILES` is the number of files (data sources) to consume, 
* `NUM_ITERATIONS` is the number of times to run the test, 
* `NUM_MESSAGES` is the total number of messages to process, 
* `DATA_PATH` is the location of the data files, 
* `POOLSIZE` is the number of threads to use for processing (0 = dedicated threads; one per source, any value > 0 will
use the managed Ziploq thread pool with the specified value of threads), and 
* `BUFSIZE` is the buffer size of the input stream for each file. Using a high buffer size value such as 8192 when 
reading from many files will make the app quiet memory-hungry, therefore it is recommended to also specify a relevant 
`Xmx` value.

### Leave feedback

Feel free to reach out if you have any questions or queries! For issues, please use Github's issue 
tracking functionality.

 ----

Måns Tegling, 2024