<link rel='stylesheet' href='../assets/css/main.css'/>

[<< back to main index](../README.md)

Lab 2 : Kafka Command Line Utilities
=====================

### Overview
Use Kafka Command line utils

### Depends On
None

### Run time
10 mins


## Step 1 : Open two terminals to your Kafka node

<img src="../assets/images/2a.png" style="border: 5px solid grey ; max-width:100%;"  />

## Step 2 : Create Topics
Inspect current topics

```bash
    $   ~/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --list
```

Let's create a `test` topic

```bash
    $   ~/kafka/bin/kafka-topics.sh  --zookeeper localhost:2181  --create --topic test --replication-factor 1  --partitions 2
```

Verify:
```bash
    $   ~/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --list


    $   ~/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --describe --topic test
```

## Step 3 : Inspecet Kafka Manager UI

<img src="../assets/images/2b.png" style="border: 5px solid grey ; max-width:100%;"  />

## Step 4 : Let's send some messages

On terminal-1  start `kafka-console-producer`

```bash
    $    ~/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test
```

On terminal-2 start `kafka-console-consumer`
```
    $   ~/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test
```

**==> In producer terminal (1) type some data**  

**==> Watch the output on consumer terminal (2)**

Click on the image to see larger version.
<a href="../assets/images/2c.png"><img src="../assets/images/2c.png" style="border: 5px solid grey ; max-width:100%;"  /></a>


## Step 5 : Try these
Start consumer with `from-beginning` flag
```
    $    ~/kafka/bin/kafka-console-consumer.sh --bootstrap-servers localhost:9092 --topic test --from-beginning
```

**=> Is the consumer reading the data 'in-order' as we typed in producer terminal?  Why or why not?**  


## Step 6: Find out options for console-producer and console-consumer
Invoke `console-producer` and `console-consumer` without any arguments to find out all the options.

```bash
    $    ~/kafka/bin/kafka-console-producer.sh

    $    ~/kafka/bin/kafka-console-consumer.sh

```

## [Optional] Delete a topic
Create a topic called `x`.  
Delete it.

Hint : look through the options in `kafka-topics.sh`
```bash
  $   ~/kafka/bin/kafka-topics.sh
```
