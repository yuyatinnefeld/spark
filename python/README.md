<h1 align="center">🤓 PySpark 🤓 </h1> <br>
<h2>🐍 Table of Contents 🐍</h2>

- [About](#about)
- [Benefit](#benefit)
- [Info](#info)
- [Setup](#setup)
- [MLlib](#MLlib)

## About
PySpark is an interface for Apache Spark in Python.
It not only allows you to write Spark applications using Python APIs, but also provides the PySpark shell for interactively analyzing your data in a distributed environment. PySpark supports most of Spark’s features such as Spark SQL, DataFrame, Streaming, MLlib (Machine Learning) and Spark Core.


## Benefit
- quick to build the spark work env
- well common workspace for DS & DE
- easy to learn & test
- great to create prototypes => native scala spark

## Info
- https://spark.apache.org/docs/latest/api/python/

## Setup
### 0. activate venv
```bash
python -m venv venv
source ./venv/bin/activate (Mac) or venv\Scripts\activate (Windows)
```

### 1. install the packages

```bash
pip install -r requirements.txt
```

### 2. run spark_performance.py to compare the py native vs. spark performance

```bash
mkdir project
touch project/spark_performance.py
```

```bash
python project/spark_performance.py
```

## MLlib
MLlib is Spark’s machine learning (ML) library. 
Its goal is to make practical machine learning scalable and easy. 

You can learn the following ML skills:
 - Data Preprocessing
   - Numeric Func=> (MinMaxScaler/Normalizer, StandardScaler, Bucketizer)
   - Text Fun => (Tokenizer, HashingTF)
 - Clustering
   - K-means (small/mid-size dataset)
   - Hierarchical clustering (large-size dataset)
 - Classification
   - Naive Bayes
   - Multilayer perceptron (MLP)
   - Decision tree
 - Regression
   - Linear regression
   - Decision tree regression
   - Gradient-boosted tree regression
 - Recommendations
   - Collaborative filtering