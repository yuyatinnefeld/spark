# EMR (Elastic Map Reduce)

## Info
- https://lynnlangit.medium.com/scaling-custom-machine-learning-on-aws-part-3-kubernetes-5427d96f825b

## Create a EMR cluster
- EMR cluster version: use emr-5.29.0 (another versions have a bug and you cannot use notebook) 
- Applications: Spark: Spark 2.4.7 on Hadoop
- Define the instance number (1 NameNode (Master) + n DateNode (Slave))
- Select EC2 Key pair
- Permission: Default
- The launch takes 5-10 min
- When the status shows "Wating Cluster ready" then done! 

## Creata a Notebook
- Select the EMR cluster
- Default setup

## Use the Notebook
1. Start the notebook > status = ready
2. Click open in Jupyter
3. Change the Kernel to use the pyspark environment
4. Copy paste the pyspark code:
- https://github.com/lynnlangit/learning-hadoop-and-spark/blob/master/5-Use-Spark/PySpark/calculatePi.py
5. Test run the pyspark code

![GitHub Logo](/images/jupyternotebook.png)


## Performance Tuning
### Spark History Server
1. check the Jobs section
- Job > Event Timeline
- Job > Dag to check the details
2. check the Stage section
3. Environment > spark.driver.memory Important!

### EMR Clusters (Hadoop)
1. check eC2 intance performance

### Monitoring cluster 
- CloudWatch
- AWS EC2 spot or batch instance

## Data Optiminzing
- Data format (Compression, encryption)
- Data partitions (size, count)
- Data quality (nulls, invalid values, errors)
- Data skew (Ranges of values)

## Scaling Spark Cloud Data Tier
1. Saas for test (ex. Databricks)
2. Paas for prod (ex. ERM, Dataproc)
3. Iass for optimize cost, performance etc. (ex. EKS Docker + S3, GKE Docker + Cloud Storage)

You can also buy a finished product/setup by AWS marketplace (e.g. VariantSpark Notebook) and reverse engineer this


