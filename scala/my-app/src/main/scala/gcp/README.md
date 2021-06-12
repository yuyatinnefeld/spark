#Spark + Dataproc + Cloud Storage

Info: https://cloud.google.com/dataproc/docs/tutorials/gcs-connector-spark-tutorial#scala

## create a GCP project
> https://console.cloud.google.com/

## define the 4 env variables
```bash
PROJECT=project-id
BUCKET_NAME=bucket-name
CLUSTER=cluster-name
REGION=cluster-region
ZONE=cluster-zone
STORAGE_CLASS=STANDARD
```

## create GCP SDK container 
```bash
docker pull gcr.io/google.com/cloudsdktool/cloud-sdk:latest
```

## run docker container
```bash
docker container start gcloud-config
```

## define alias
```bash
alias gcp='docker run --rm --volumes-from gcloud-config gcr.io/google.com/cloudsdktool/cloud-sdk'
```

## test
```bash
gcp gcloud version
```

## login GCP
```bash
docker exec -it gcloud-config gcloud auth login
```

## config setup
```bash
gcloud config set project ${PROJECT}
```

## create a bucket
```bash
gsutil mb -p ${PROJECT} -c ${STORAGE_CLASS} -l ${REGION} -b on gs://${BUCKET_NAME}
```

## region setup (germany)
```bash
gcloud compute project-info add-metadata \
--metadata google-compute-default-region=${REGION},google-compute-default-zone=${ZONE}
```
 
## create a dataproc cluster
```bash
gcloud dataproc clusters create ${CLUSTER} \
--project=${PROJECT} \
--region=${REGION} \
--single-node
```

## upload a sample file into the storage bucket
```bash
gsutil cp gs://pub/shakespeare/rose.txt \
gs://${BUCKET_NAME}/input/rose.txt
```
check
```bash
gsutil ls
```

## create scala object
```scala
package gcp

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object WordCount {
def main(args: Array[String]) {
if (args.length != 2) {
throw new IllegalArgumentException(
"Exactly 2 arguments are required: <inputPath> <outputPath>")
}

    val inputPath = args(0)
    val outputPath = args(1)

    val sc = new SparkContext(new SparkConf().setAppName("Word Count"))
    val lines = sc.textFile(inputPath)
    val words = lines.flatMap(line => line.split(" "))
    val wordCounts = words.map(word => (word, 1)).reduceByKey(_ + _)
    wordCounts.saveAsTextFile(outputPath)
}
}
```
## build package
```bash
sbt clean package
```

## stage the package to cloud storage
```bash
gsutil cp target/scala-2.12/my-app_2.12-1.0.jar \
gs://${BUCKET_NAME}/scala/my-app_2.12-1.0.jar
```

## run the spark wordcount app
```bash
gcloud dataproc jobs submit spark \
--cluster=${CLUSTER} \
--class=gcp.CloudStorageWordCount \
--jars=gs://${BUCKET_NAME}/scala/my-app_2.12-1.0.jar \
--region=${REGION} \
-- gs://${BUCKET_NAME}/input/ gs://${BUCKET_NAME}/output/
```

read the result from cloud storage bucket
```bash
gsutil cat gs://${BUCKET_NAME}/output/*
```