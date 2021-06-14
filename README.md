<h1 align="center">Apache Spark</h1> <br>
<h2> 🚀 Table of Contents 🚀 </h2>

- [About](#about)
- [Benefit](#benefits)
- [Info](#info)
- [Components](#components)
- [Setup](#setup)

## About
Apache Spark is a data processing framework that can quickly perform processing tasks on very large data sets, and can also distribute data processing tasks across multiple computers, either on its own or in tandem with other distributed computing tools.

Apache Spark is written in Scala and because of its scalability on JVM - Scala programming is most prominently used programming language, by big data developers for working on Spark projects. Developers state that using Scala helps dig deep into Spark’s source code so that they can easily access and implement the newest features of Spark. Scala’s interoperability with Java is its biggest attraction as java developers can easily get on the learning path by grasping the object oriented concepts quickly.

## Benefit
You can learn in this project:
- Row Level (RDD) & Structured API (DataFrame, DataSet, SQL)
- [Spark Configuration](https://github.com/yuyatinnefeld/spark/tree/master/scala) 
- Spark Ecosystem 
    - [Spark Core RDD](https://github.com/yuyatinnefeld/spark/tree/master/scala/my-app/src/main/scala/com/spark_rdd)
    - [Spark SQL (DataFrame & DataSet)](https://github.com/yuyatinnefeld/spark/tree/master/scala/my-app/src/main/scala/com/spark_sql)
    - [Spark MLlib](https://github.com/yuyatinnefeld/spark/tree/master/scala/my-app/src/main/scala/com/spark_mllib)
    - [Spark Streaming (Twitter & Kafka)](https://github.com/yuyatinnefeld/spark/tree/master/scala/my-app/src/main/scala/com/spark_streaming)
    - [Structured-Streaming](https://github.com/yuyatinnefeld/spark/tree/master/scala/my-app/src/main/scala/com/structured_streaming)
- [Spark Docker](https://github.com/yuyatinnefeld/spark/tree/master/kafka-docker)
- [Spark Notebook](https://github.com/yuyatinnefeld/spark/tree/master/scala/my-app/src/main/scala/spark_note)
- [Spark Deploy](https://github.com/yuyatinnefeld/spark/tree/master/scala/my-app/src/main/scala/spark_deploy) 
- [Spark GCP](https://github.com/yuyatinnefeld/spark/tree/master/scala/my-app/src/main/scala/gcp)

## Info
- https://spark.apache.org/docs/latest/
- https://intellij-support.jetbrains.com/hc/en-us

### Spark provides all data Types
- unstructured data (schema-never)
- semi-structured data (schema-later)
- structured data (schema-first)

## Setup
### [Scala](https://github.com/yuyatinnefeld/spark/tree/master/scala)
### [Python](https://github.com/yuyatinnefeld/spark/tree/master/python)
### [Java](https://github.com/yuyatinnefeld/spark/tree/master/java)
### [Kafka-Docker](https://github.com/yuyatinnefeld/spark/tree/master/kafka-docker)

## Benefits
Apache Spark has many features which make it a great choice as a big data processing engine. Many of these features establish the advantages of Apache Spark over other Big Data processing engines. Spark has the following beneftis:
- Fault tolerance
- Great Documentation & Community
- Cost Efficient (OSS)
- Dynamic In Nature
- Lazy Evaluation
- Real-Time Stream Processing & Batch Processing
- Speedy Data Processing (100x faster than Hadoop MapReduce)
- Suitable for Advanced Analytics
- API for Multiple languages (Scala, R, Python, Java, SQL)
- Integrated with Hadoop/k8s/Mesos

### Spark Application Logic
![GitHub Logo](/images/spark-components.png)

Spark applications run as independent sets of processes on a cluster, coordinated by the SparkContext object in your main program (called the driver program).

Specifically, to run on a cluster, the SparkContext can connect to several types of cluster managers (either Spark’s own standalone cluster manager, Mesos or YARN), which allocate resources across applications. Once connected, Spark acquires executors on nodes in the cluster, which are processes that run computations and store data for your application.

#### Cluster Manager
- Spark (Standalone)
- Apache Mesos
- Hadoop YARN
- Kubernetes