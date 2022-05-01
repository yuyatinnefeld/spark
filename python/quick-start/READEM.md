1. setup 
```bash
# install docker-compose file
curl -LO https://raw.githubusercontent.com/bitnami/bitnami-docker-spark/master/docker-compose.yml

# test run
docker-compose up -d
docker ps

# open spark master node
curl local:8080

# reset
docker-compose down

# udpate docker volume (edit the line 13 to update volume location)

# restart 
docker-compose up
docker exec -u 0 -it spark-master bash 

# if spark-submit exec not working 
docker exec -it spark-master bash

# verify the python files
cd /opt/bitnami/spark/examples/src/main/python/example_module

# run spark job
export SPARK_HOME=/opt/bitnami/spark
spark-submit /opt/bitnami/spark/examples/src/main/python/example_module/main.py
# run pytest
#TODO

```