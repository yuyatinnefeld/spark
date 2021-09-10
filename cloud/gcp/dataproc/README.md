# Dataproc

## create a cluster
```bash
PROJECT_ID=$(gcloud config get-value project)
CLUSTER_NAME="yt-cluster"
REGION="europe-west3"
WORKERS=5

# connect to your GCP project
gcloud config set project ${PROJECT_ID}

# create dataproc clusters
# GCE instances + cloud storage instances are created
gcloud dataproc clusters create ${CLUSTER_NAME} --region=${REGION}
```

## submit a demo job
```bash
gcloud dataproc jobs submit spark --cluster example-yt-cluster \
    --region=region \
    --class org.apache.spark.examples.SparkPi \
    --jars file:///usr/lib/spark/examples/jars/spark-examples.jar -- 1000
```

## update a cluster (increase workers)

```bash
gcloud dataproc clusters update ${CLUSTER_NAME} \
    --region=region \
    --num-workers ${WORKERS}
```

## run spark job run with a custom jar file

1. write a custom jar file
2. store this into the GCS bucket
3. select this jar file and class by the setting up "dataproc jobs submit"

```bash
gcloud dataproc jobs submit spark --cluster example-yt-cluster \
    --region=region \
    --class org.apache.spark.examples.XXX \
    --jars gs://xxxx-bucketusr/helloSpark.jar -- 1000
```

## run spark job in the dataproc Web UI
1. ssh connect to a cluster instance (GCE) 
```bash
PROJECT_ID=$(gcloud config get-value project)
CLUSTER_NAME="yt-cluster"
GCE_INSTANCE="yt-cluster-m"
ZONE="europe-west3"

# connect to your GCP project
gcloud config set project ${PROJECT_ID}

# ssh connect
gcloud beta compute ssh --zone ${ZONE} ${GCE_INSTANCE} --project ${PROJECT_ID}

# start spark session
spark-shell

# read a file from the GCS bucket
val text_file = sc.textFile("gs://pub/shakespeare/rose.txt")

# run a wordcoutn mapreduce
val wordCounts = text_file.flatMap(line => line.split(" ")).map(word =>
(word, 1)).reduceByKey((a, b) => a + b)

# check the result
wordCounts.show()

# save the result
wordCounts.collect

# store the result into the GCS bucket
wordCounts.saveAsTextFile("gs://yygcplearning-ds/wordcounts-out/")

exit

# check the outputs in the bucket
gsutil ls gs://yygcplearning-ds/wordcounts-out/

# show the result
gsutil cat gs://yygcplearning-ds/wordcounts-out/part-00000

# delete the data
gsutil rm -r gs://yygcplearning-ds/wordcounts*
```

## delete a cluster
```bash
gcloud dataproc clusters delete ${CLUSTER_NAME} \
    --region=region

# delete GCS bucket instances
gsutil rm -r gs://dataproc-staging*
gsutil rm -r gs://dataproc-temp*
```