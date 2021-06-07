# 🤓 Spark Notebook 🤓
![GitHub Logo](/images/zeppelin1.png)

## Why Notebook?
- quick to build the spark work env
- well common workspace for DS & DE
- easy to learn & test
- Python, Scala, R provided
- great to create prototypes => later native scala code

## Jupyter Notebook + Docker (Opensource)
```bash
docker pull jupyter/all-spark-notebook
```
```bash
docker run -it --rm -p 8888:8888 jupyter/all-spark-notebook
```
```bash
http://127.0.0.1:8888/?token=....
```

## Zeppelin + Docker (Opensource)

```bash
docker run -p 8080:8080 --rm --name zeppelin apache/zeppelin:0.9.0
```
```bash
http://127.0.0.1:8080
```

If you want to specify logs and notebook dir:
```bash
docker run -p 8080:8080 --rm \
-v $PWD/logs:/logs \
-v $PWD/notebook:/notebook \
-e ZEPPELIN_LOG_DIR='/logs' \
-e ZEPPELIN_NOTEBOOK_DIR='/notebook' \
--name zeppelin apache/zeppelin:<release-version> # e.g '0.7.1'
```

```scala
import org.apache.commons.io.IOUtils
import java.net.URL
import java.nio.charset.Charset

val bankText = sc.parallelize(
IOUtils.toString(
new URL("https://s3.amazonaws.com/apache-zeppelin/tutorial/bank/bank.csv"),
Charset.forName("utf8")).split("\n"))

case class Bank(age: Int, job: String, marital: String, education: String, balance: Int)

val bank = bankText.map(s => s.split(";")).filter(s => s(0) != "\"age\"").map(
s => Bank(s(0).toInt,
s(1).replaceAll("\"", ""),
s(2).replaceAll("\"", ""),
s(3).replaceAll("\"", ""),
s(5).replaceAll("\"", "").toInt
)
).toDF()

bank.registerTempTable("bank")
```

```roomsql
%sql
select age, count(1) value
from bank
where age < ${maxAge=30}
group by age
order by age
```

![GitHub Logo](/images/zeppelin1.png)

![GitHub Logo](/images/zeppelin2.png)

## Polynote (Opensource)
> https://polynote.org
> https://towardsdatascience.com/getting-started-with-polynote-netflixs-data-science-notebooks-47fa01eae156


## Data Bricks Notebook (Enterprise)
> https://docs.databricks.com/notebooks/notebooks-manage.html#