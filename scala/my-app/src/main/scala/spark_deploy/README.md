# 💪 Spark Deployment 💪

## Spark Cloud File System:
| Type  | Solution |Service Type |Provider |
| ------------- | ------------- | -------------| -------------|
| Local to VM | Worker on EC2 + Manager (Mesos / Hadoop / K8s) on EKS + S3 | Laas | AWS |
| Local to VM | Worker on GCE + Manager (Mesos / Hadoop / K8s) on GKE + GCS| Laas | GCP |
| Data Like | AWS EMR + S3  | Paas | AWS |
| Hadoop | AWS EMR HDFS  | Pass | AWS |
| Hadoop | GCP Dataproc HDFS | Pass | GCP |
| Hadoop | Databricks on AWS DFS  | Sass | Databricks |
| Hadoop | Terra.bio on GCP  | Sass | GCP |

GCP Dataflow: Apache Beam based: when you want to go serverless (don't have favor a DevOps approach to operations)
GCP Dataproc: Hadoop based: when you have dependencies on tools/packages in your Apache Hadoop / Spark ecosystem

## Development Steps:
1. Paas Choice => Laas Choice
AWS:
- step0. Import / use AWS Marketplace to learn quick cloud native fashion
- step1. Build your own Paas Soultion: ERM setup => CloudWatch templates setup
- step2. Build your own Laas Solution: ERM => (EKS + S3) => Terraform templates

GCP:
- step1: Build your own Paas Soultion: Dataproc / Data flow + Cloud Monitoring
- step2: Build your own Laas Soultion: k8s on GCP + GSC

   
## Crucial keys for the solution choosing
> Which file System? Which vendor?
- True Cost
1. Service Cost
2. Licence Cost
3. Learning Cost

## Optimize Computer Tier
### GCP
Cluster Monitoring => Stack Driver
GCE preempt-able instances
### AWS
Cluster Monitoring => CloudWatch
AWS EC2 instances or batch instances

## Setup

### GCP Dataproc
1. GCP Console (https://console.cloud.google.com/dataproc)
2. Create Project
3. Activate Dataproc API
4. Create Cluster (Name, Region, machine type => n1-stamdard-2 (2 vCPU, 7.5GB memory))
5. Create Jobs
   - Name: xxxx
   - Region: 
   - Jobtype => Spark 
   - Class => org.apache.spark.examples.SparkPi
    - Jarfile => file:///usr/lib/spark/examples/jars/spark-examples.jar
    - Optionale Komponenten: Jupyter

6. Done

### AWS EMR
1. AWS Console
2. ...
3. ...


### Run local Spark app
```bash
spark-submit --class <package_path.scala_program> <jar_file_path>
```

```bash
spark-submit --master local[8] --class com.yu.WordCount target/scala-2.12/yu-sbt-tutorial_2.12-0.1.jar
```

### Run on a Spark standalone cluster in client deploy mode
```bash
spark-submit \
--class com.yu.WordCount \
--master spark://207.184.161.138:7077 \
--executor-memory 20G \
--total-executor-cores 100 \
target/scala-2.12/yu-sbt-tutorial_2.12-0.1.jar \
1000
```

### Run on a YARN cluster
```bash
export HADOOP_CONF_DIR=XXX
spark-submit \
--class org.apache.spark.examples.SparkPi \
--master yarn \
--deploy-mode cluster \  # can be client for client mode
--executor-memory 20G \
--num-executors 50 \
/path/to/examples.jar \
1000
```
### Run on a Mesos cluster in cluster deploy mode with supervise
```bash
spark-submit \
--class <class name> \
--master mesos://207.184.161.138:7077 \
--deploy-mode cluster \
--supervise \
--executor-memory 20G \
--total-executor-cores 100 \
http://path/to/examples.jar \
1000
```

### Run on a Kubernetes cluster in cluster deploy mode
```bash
spark-submit \
--class <class name> \
--master k8s://xx.yy.zz.ww:443 \
--deploy-mode cluster \
--executor-memory 20G \
--num-executors 50 \
http://path/to/examples.jar \
1000
```
