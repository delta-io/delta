# Multi-DataSource Spark-Shell Guide
## HBase
Here we use Phoenix to read and write HBase data.  
### 1 Add Dependencies
Download hbase-2.3.5 from http://archive.apache.org/dist/hbase, extract it, then, copy the hbase*.jar files in lib to spark jars path.  
Download phoenix-hbase-2.3-5.1.2-bin from http://phoenix.apache.org/download.html, extract it, then, copy the jar files to spark jars path.  
### 2 Spark-Shell operations
#### 2.1 Read data from HBase
Read data from HBase, for instance Table TEST_DATA in hbase1, the configuration information is in configuration/hbase.propertie.     
```scala
val df = spark.read.format("hbase").option("database", "habse1").option("table", "\"TEST_DATA\"").load()
```
#### 2.2 Write data to HBase
Mode support "overwrite" and "append".  
```scala
df.write.format("hbase").mode("overwrite").option("database", "habse1").option("table", "\"TEST_DATA\"").save()
```

------

## Relational DataBase
We Support to read and write data from MySQL, Oracle and SQL Server.  
### 1 Add Dependencies
Copy the relational database JDBC drivers below to spark jars path:  
MySQL JDBC driver: mysql-connector-java-8.0.24.jar  
Oracle JDBC driver: ojdbc8-19.11.0.0.jar  
SQL Server JDBC driver: mssql-jdbc-9.2.1.jre8.jar
### 2 Spark-Shell operations
#### 2.1 Read data from MySQL
Read data from Mysql, for instance Table test_data in mysql1, the configuration information is in configuration/relationaldb.propertie.  
```scala
val df = spark.read.format("relationaldb").option("database", "mysql1").option("table", "test_data").load()
```
#### 2.2 write data to MySQL
Mode support "overwrite" and "append".  
```scala
df.write.format("relationaldb").mode("overwrite").option("database", "mysql1").option("table", "test_data").save()
```
#### 2.3 Other relational databases
For Oracle and SQL Server, just to modify the parameters in load() and save().

------

## Common File

### 1 Content overview

The commonFile data source likes the spark binary file data source.
It reads binary files and converts each file into a single record that contains the raw content and metadata of the file, but the schema is different

Schema like that

|  filename  |    path    |   suffix   |   size   |                      commonFile                      |
| :--------: | :--------: | :--------: | :------: | :--------------------------------------------------: |
| StringType | StringType | StringType | LongType | StructFiled[ StructedType(BinaryType,BinaryType,……)] |

It uses the StructType composed by a series of BinaryType to contain raw content and it also supports spark structStreaming.

- you can specify the number of the BinaryType in the commonFile filed by properties file

### 2 Spark-Shell operations

#### 2.1 batch read

```scala
val dataFrame = spark.read.format("commonFile").load("/tmp/commonFile/")
```

#### 2.2 batch write

```scala
dataFrame.write.format("commonFile").save("/tmp/commonFile/")
```

#### 2.3 streaming read

```scala
val frame = spark.readStream.format("commonFile").load("/tmp/commonFile")
```

#### 2.4 streaming write

```scala
frame.writeStream.format("commonFile").option("path", "/tmp/commonFile")
  .option("checkpointLocation", "/tmp/commonFile").start()
```

------

## Remote File

### 1 Content overview

It reads remote computer's binary files and converts each file into a single record that contains the raw content and metadata of the file.
The schema is same to the commonFile data source

### 2 Add Dependencies

Put the j2ssh-maverick-1.5.5.jar file in the jars folder of spark

### 3 Spark-Shell operations

#### 3.1 Configure the necessary parameters ip, port, user name and password

```scala
val ip = "ip"
val port = 22
val username = "username"
val password = "password"
```

#### 3.2 batch read

```scala
val readFromRemote = spark.read.format("remoteFile")
  .option("ip", ip).option("port", port).option("username", username).option("password", password)
  .load("/tmp/remoteFile/")
```

#### 3.3 batch write

```scala
readFromRemote.write.format("remoteFile")
  .option("ip", ip).option("port", port).option("username", username).option("password", password)
  .save("/tmp/remoteFileWrite/")
```

#### 3.4 use sql statement

- create tmp view

```scala
readFromRemote.createOrReplaceTempView("test")
```

- sql statement

```scala
spark.sql("select * from test").show()
```

```scala
spark.sql("select fileName,path from test").show()
```

```scala
spark.sql("select fileName,path,size from test where size=7").show()
```
