### Major Modifications (under hadoop-mapreduce-project)

* pseudo random partitino assignment - done

* intermediate file encryption - done (directly invoke hadoop intermediate encryption flag; we can have out own implementation if time permits)

* dummy records padding - in progress

Search keyword "COS518 Edition" in the following files, you will see the marked changes. 

Currently the code is still messy as I added a lot of print messages and am in the progress of implementing dummy padding. I will clean things after the major functionalities are completed. 

#### Under hadoop-mapreduce-client-core

mapreduce/Job.java: setMelbourneShuffle()

mapred/JobConf.java: setMelbourneShuffleKey(), getMelbourneShuffleKey()

mapreduce/MRJobConfig.java: MELBOURNE_KEY

mapred/MapTask.java: in NewOutputCollector class: PRNG(), PRF(), and getPartitions()

#### Under hadoop-mapreduce-examples 

examples/WordCount.java: main()