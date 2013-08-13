AvroToolbox
===========

ArcGIS toolbox to store feature classes in HDFS in [Apache Avro](http://avro.apache.org) and [Parquet format](http://parquet.io)

A GIS generic feature with geometry and attributes is generated based on the [Avro specification](http://avro.apache.org/docs/current/spec.html) defined in AvroFeature.avsc file.

## Compiling the schema

    $ mvn avro:schema

This enables us to have a concrete java POJO class to work with.

## CDH3 Packaging

    $ mvn -Pcdh3 clean package

## CDH4 Packaging

    $ mvn -Pcdh4 clean package

Copy the file target/AvroToolbox-1.0-SNAPSHOT.jar and the folder target/libs to C:\Program Files (x86)\ArcGIS\Desktop10.1\java\lib\ext

![Export To Avro](https://dl.dropboxusercontent.com/u/2193160/ExportToAvro.png "Export To Avro")

The following is a sample content of a Hadoop properties file:

    fs.default.name=hdfs\://localhadoop\:9000
    hadoop.socks.server=localhost\:6666
    hadoop.rpc.socket.factory.class.default=org.apache.hadoop.net.SocksSocketFactory
    dfs.client.use.legacy.blockreader=true

## Running MapReduce job

    $ mvn -Pcdh3-job clean package
    $ hadoop jar target/AvroToolbox-1.0-SNAPSHOT-job.jar /user/mraad_admin/worldlabels.avro /user/mraad_admin/output

## Viewing Avro content

    $ mvn -Pcdh3 clean package
    $ mvn -Pcdh3 exec:java -q -Dexec.mainClass=com.esri.AvroToJson -Dexec.args="hdfs://localhadoop:9000/user/mraad_admin/output/part-00000.avro"

## Experiments

I'm using the [Cloudera Quick Start VM](http://www.cloudera.com/content/support/en/downloads/download-components/download-products.html?productID=F6mO278Rvo) for my local testing.
[Apache Hive](http://hive.apache.org/) is built into that VM.
The following is a small experiment to load Avro features into HDFS and query that set of features using Hive.

From the VM Terminal Shell, create a CDH4 package of the project:

    $ mvn -Pcdh4 clean package

Create a *points* folder in HDFS, and load a set of random points as Avro features in HDFS:

    $ hadoop fs -mkdir points
    $ mvn exec:java -q -Dexec.mainClass=com.esri.RandomPoints -Dexec.args="cloudera hdfs://localhost.localdomain:8020/user/cloudera/points/points.avro"

Place the Avro schema in HDFS:

    $ hadoop fs -put src/main/avro/AvroPointFeature.avsc points.avsc

Start the Hive shell and define the table:

    $ hive
    hive> CREATE EXTERNAL TABLE points
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
    STORED AS
    INPUTFORMAT  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
    LOCATION '/user/cloudera/points/'
    TBLPROPERTIES ('avro.schema.url'='hdfs://localhost.localdomain/user/cloudera/points.avsc');

Query the table:

    hive> select geometry.coord,attributes['id'] from points limit 10;
    Total MapReduce jobs = 1
    Launching Job 1 out of 1
    Number of reduce tasks is set to 0 since there's no reduce operator
    Starting Job = job_201308080639_0002, Tracking URL = http://localhost.localdomain:50030/jobdetails.jsp?jobid=job_201308080639_0002
    Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_201308080639_0002
    Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 0
    2013-08-08 07:28:45,692 Stage-1 map = 0%,  reduce = 0%
    2013-08-08 07:28:50,726 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 1.13 sec
    2013-08-08 07:28:51,736 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 1.13 sec
    2013-08-08 07:28:52,744 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 1.13 sec
    MapReduce Total cumulative CPU time: 1 seconds 130 msec
    Ended Job = job_201308080639_0002
    MapReduce Jobs Launched:
    Job 0: Map: 1   Cumulative CPU: 1.13 sec   HDFS Read: 15935 HDFS Write: 486 SUCCESS
    Total MapReduce CPU Time Spent: 1 seconds 130 msec
    OK
    {"x":159.74572199827702,"y":40.32521537633818}	{1:0}
    {"x":-149.73039877969308,"y":-13.94736096833141}	{1:1}
    {"x":46.361639376801236,"y":25.77145412669374}	{1:2}
    {"x":-132.41313972636817,"y":44.22558416016216}	{1:3}
    {"x":-8.925429128076644,"y":29.313162418474704}	{1:4}
    {"x":62.46805700709206,"y":40.81419992554635}	{1:5}
    {"x":125.16119385803438,"y":-31.717399080676735}	{1:6}
    {"x":89.4502093945652,"y":-32.22107550052377}	{1:7}
    {"x":-3.5726678441100717,"y":-66.01880710966441}	{1:8}
    {"x":40.776519552978755,"y":61.298035708039976}	{1:9}
    Time taken: 7.51 seconds
