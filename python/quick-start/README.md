1. setup 
```bash
# install docker-compose file
curl -LO https://raw.githubusercontent.com/bitnami/bitnami-docker-spark/master/docker-compose.yml

# test run
export PWD=$(pwd)
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
cd /opt/bitnami/spark/examples/src/main/python/src/example_module

# run spark job

export SPARK_HOME=/opt/bitnami/spark
spark-submit $SPARK_HOME/src/example_module/main.py

# setup for pytest
# check the py4j-zip info
cd /opt/bitnami/spark/python/lib/
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.3-src.zip:$PYTHONPATH

# run pytest
cd $SPARK_HOME/src
pip install pytest
pytest tests -rA
```