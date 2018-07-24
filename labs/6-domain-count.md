<link rel='stylesheet' href='../assets/css/main.css'/>

[<< back to main index](../README.md)

Lab 6: Domain Count
===========================

### Overview
Count Domain based stats of clickstream

### Depends On

### Run time
20 mins

## Step 1 : Create a 'clickstream' topic
```bash
    $   ~/kafka/bin/kafka-topics.sh  --zookeeper localhost:2181  --create --topic clickstream --replication-factor 1  --partitions 2
```

## Step 2 : Clickstream Producer
* Inspect file and make any fixes : `src/main/java/x/utils/ClickStreamProducer.java`  
* Run the producer in Eclipse, Right click on the file and run as 'Java Application'
* Make sure it is sending messages as follows
  - key : Domain
  - value : clickstream data
  - example  :
  ```
  key=facebook.com, value={"timestamp":1451635200005,"session":"session_251","domain":"facebook.com","cost":91,"user":"user_16","campaign":"campaign_5","ip":"ip_67","action":"clicked"}
  ```

## Step 3 :  DomainCount Consumer
This consumer will keep an running total of domain count seen in clickstream.

* Inspect file : `src/main/java/x/lab6_domain_count/DomainCountConsumer.java`  
* Fix the TODO items
Use reference Java API [for Consumer](https://kafka.apache.org/0100/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html)



## Step 4 :Run
* Run the `lab6_domain_count.DomainCountConsumer` in Eclipse,
* Run the `utils.ClickStreamProducer` in Eclipse,
* Expected output

```
Got 10 messages
Received message : ConsumerRecord(.....
Domain Count is
  [facebook.com=1]

Received message : ConsumerRecord(.....
Domain Count is
  [facebook.com=1, foxnews.com=1]

Received message : ConsumerRecord(.....
Domain Count is
  [facebook.com=2, foxnews.com=1]
...
```
