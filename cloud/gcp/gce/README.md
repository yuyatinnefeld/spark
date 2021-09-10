# GCE + pyspark docker image

## Setup a docker vm instance on GCP

### create a container-optimized GCE instance
```bash
PROJECT_ID=$(gcloud config get-value project)
INSTANCE_NAME="pyspark-docker"
ZONE="europe-west3-b"
IMAGE_NAME="cos-cloud"

# connect to your GCP project
gcloud config set project ${PROJECT_ID}

# create a container GCE vm instance
gcloud compute instances create-with-container ${INSTANCE_NAME} \
    --container-image ${IMAGE_NAME} --zone=${ZONE}

# ssh connect
gcloud beta compute ssh --zone ${ZONE} ${INSTANCE_NAME} --project ${PROJECT_ID}

# test docker
sudo docker ps
sudo docker run hello-world
```

### create a GCE instance and install docker (alternative solution)
```bash
PROJECT_ID=$(gcloud config get-value project)
INSTANCE_NAME="pyspark-docker"
ZONE="europe-west3-b"

# connect to your GCP project
gcloud config set project ${PROJECT_ID}

# create a GCP vm instance
gcloud compute instances create ${INSTANCE_NAME} --zone=${ZONE}

# ssh connect
gcloud beta compute ssh --zone ${ZONE} ${INSTANCE_NAME} --project ${PROJECT_ID}

# install docker engine
sudo apt-get update
sudo apt-get install docker.io

# test docker
sudo docker ps
sudo docker run hello-world
```

## Create a pyspark container
```bash
docker container run --name pyspark -p 8888:8888 jupyter/pyspark-notebook
```

1. open the jupyter notebook URL which is displayed in your terminal
- http://127.0.0.1:8888/?token=xxxxx

2. paste the token and set new password

3. create a new notebook or import your notebook

## Use Pyspark in the vm instance


```bash
# container activate
docker contaienr start pyspark

# check the container is running
docker ps

# start docker container bash session
docker exec -it pyspark bash

# start pyspark session
pyspark

Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.1.2
      /_/

Using Python version 3.9.6 (default, Jul 11 2021 03:39:48)
Spark context Web UI available at http://a9e582d60d90:4040
Spark context available as 'sc' (master = local[*], app id = local-1631084757428).
SparkSession available as 'spark'.

>>> sc
<SparkContext master=local[*] appName=PySparkShell>

>>> spark
<pyspark.sql.session.SparkSession object at 0x7f0faa0064c0>

exit
```

```bash
vi pi_calculator.py
```

```python
import pyspark
sc = pyspark.SparkContext('local[*]')

def inside(p):
    x, y = random.random(), random.random()
    return x*x + y*y < 1

count = sc.parallelize(range(0, NUM_SAMPLES)) \
             .filter(inside).count()
print("Pi is roughly %f" % (4.0 * count / NUM_SAMPLES))
```


```bash
python pi_calculator.py
```


## Delete instance
```bash
gcloud compute instances delete ${INSTANCE_NAME}  --zone=${ZONE}
```