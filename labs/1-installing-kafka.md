<link rel='stylesheet' href='../assets/css/main.css'/>

[<< back to main index](../README.md)

# Lab 1 : Install Kafka

### Overview
Install and run Kafka

### Depends On
None

### Run time
1 hr

## Options for Installing Kafka

### Option 1:  Native install

This guide walks you through installing Kafka **natively** on your machine.  It assumes you have a 'unix like' system (Linux / Mac / Windows with Linux subsystem).

### Option 2: Docker

This assumes that you can run Docker containers on your machine and you have some basic knowledge of Docker.

Another option is to run our docker image.  This image has all the software (Kafka, Spark ..etc) pre-installed and configured.

Lot of people prefer this, as it has pretty much every thing you need to run the labs with no effort installing and configuring components.

Follow the [docker training image documentation](https://hub.docker.com/r/elephantscale/es-training) for details and instructions.

## STEP 0: To Instructor
Walk through this project on screen first.

## Step 1 : Login to your Kafka node
* Login using SSH
* And login via web UI
Instructor will provide the details.


## Step 2 : Run Zookeeper
Follow   [1.1-zookeeper.md](1.1-zookeeper.md)


## Step 3 : Run Kafka
Follow   [1.2-kafka.md](1.2-kafka.md)