# Sentiment Analysis on Yelp Dataset 

Repo for CSYE7200 Big-Data Sys Engr Using Scala Final Project

Project: Sentiment Analysis on Yelp Dataset using Spark Structured Streaming and Kafka

Team members:

Keshav Vyas - vyas.ke@husky.neu.edu

Akshay Patel - patel.ak@husky.neu.edu

# Introduction
![]()
# Architecture

# Project Setup

## Run Kafka and Zookeeper
Navigate to the kafka directory and execute the following commands each in different cmd terminal:

(1) bin/zookeeper-server-start.sh config/zookeeper.properties
(2) bin/kafka-server-start.sh config/server.properties
(3) bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic ReviewTopic
(4) bin/kafka-console-producer.sh --broker-list localhost:9092 --topic ReviewTopic

## Run Scala Project


## Run Play UI Project
