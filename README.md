# Hadoop MapReduce + Melbourne Shuffle

[The Melbourne shuffle](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/MSR-TR-2015-70.pdf) includes three core functionalities: 

* intermediate file encryption: Mappers encrypt intermediate key value pairs and store them on the local disk, which are later read and decrypted by Reducers. 

* pseudo random partition assignment: Given a user-provided secret key, the partition assignment of every key value pair is indistinguishable from a truly random assignment to any outside obeserver without knowing the key. This makes the partition assignment less predictable than a simple hashing algorithm (e.g. key.hashCode() used in the current Hadoop mapreduce).

* dummy records padding: Every Mapper pads its own intermediate output files (one for each partition) upto some equal size.

The first feature is already implemented in [Hadoop Mapreduce](https://hadoop.apache.org/docs/r2.10.0/hadoop-mapreduce-client/hadoop-mapreduce-client-core/EncryptedShuffle.html) by simply setting *mapreduce.job.encrypted-intermediate-data job* property to true in the corresponding configuration file. 

This project integrates the latter two features into the source code of Hadoop 2.10.0 mapreduce module.  


## Major Code Modifications 

All changes are highlighted with "COS518" comments. Below is a summary of major files being modified. 

#### Under hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop

```
mapred/JobConf.java: setMelbourneShuffleKey(), getMelbourneShuffleKey()

mapred/MapTask.java: PRNG(), PRF(), getPartitions(), flush(), collectDummies(), collect()

mapred/ReduceTask.java: runNewReduce()

mapreduce/MRJobConfig.java: MELBOURNE_KEY

mapreduce/Job.java: setMelbourneShuffle()

mapreduce/task/ReduceContextImpl.java: nextKeyValue()
```

#### Under hadoop-mapreduce-examples/src/main/java/org/apache/hadoop/ 

```
examples/WordCount.java: main()
```

## How To Replicate

To run this modified version of mapreduce, download [Hadoop 2.10.0 source code](https://archive.apache.org/dist/hadoop/common/hadoop-2.10.0/hadoop-2.10.0-src.tar.gz). 

Replace **hadoop-mapreduce-project** folder under its root directory with this repository. 

Navigate to the root directory and compile the entire source code of Hadoop. 

Note: Check hadoop-mapreduce-examples/src/main/java/org/apache/hadoop/examples/WordCount.java to see how to invoke the Melbourne shuffle.


