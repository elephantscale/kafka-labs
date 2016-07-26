<link rel='stylesheet' href='../assets/css/main.css'/>

[<< back to main index](../README.md) 

Lab 4: Producer Benchmarking
===========================

### Overview
Understand different send methods in producer

### Depends On 
lab 3

### Run time
30 mins


## Step 1 : Producer
* Inspect file : `src/main/java/x/lab_4/BenchmarkProducer.java`  
* Fix all TODO items, using Eclipse (or any other editor).



## Step 2 : Run the producer
In Eclipse, 
* Right click on 'src/main/java/x/lab_3/BenchmarkProducer.java'
* Run as 'Java Application'

In Eclipse console, you should see output as follows:
```console
== ClickstreamProducer (topic=clickstream, maxMessages=100, sendMode=SYNC) done.  100 messages sent in 23.0206651 milli secs.  Throughput : 4343.923147554933 msgs / sec

== ClickstreamProducer (topic=clickstream, maxMessages=100, sendMode=ASYNC) done.  100 messages sent in 20.3356646 milli secs.  Throughput : 4917.468986973752 msgs / sec

== ClickstreamProducer (topic=clickstream, maxMessages=100, sendMode=FIRE_AND_FORGET) done.  100 messages sent in 20.8203518 milli secs.  Throughput : 4802.992810140701 msgs / sec

```


## Step 3 : Class Discussion
Discuss your findings.

## Bonus Lab: Try different parameters