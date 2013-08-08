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

Create a *points* folder in HDFS, and load a set or random points as Avro features in HDFS:

    $ hadoop fs -mkdir points
    $ mvn -Pcdh4 exec:java -q -Dexec.mainClass=com.esri.RandomPoints -Dexec.args="hdfs://localhost:8020/user/cloudera/points/points.avro"

Place the Avro schema in HDFS:

    $ hadoop fs -put src/main/avro/AvroFeature.avsc AvroFeature.avsc

Start the Hive shell and define the table:

    $ hive
    hive> CREATE EXTERNAL TABLE points
              ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
              STORED AS
              INPUTFORMAT  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
              OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
              LOCATION '/user/cloudera/points/'
              TBLPROPERTIES ('avro.schema.url'='hdfs://localhost.localdomain/user/cloudera/AvroFeature.avsc');

Query the table:

    hive> select geometry,attributes['id'] from points limit 10;
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
    {0:{"spatialreference":{"wkid":4326},"coord":{"x":-131.67325848386128,"y":55.21639737095859}}}	{1:0}
    {0:{"spatialreference":{"wkid":4326},"coord":{"x":50.53089121219577,"y":-45.46385957292893}}}	{1:1}
    {0:{"spatialreference":{"wkid":4326},"coord":{"x":-68.66292877438813,"y":-47.99221707253536}}}	{1:2}
    {0:{"spatialreference":{"wkid":4326},"coord":{"x":151.06323974650468,"y":18.089597508541587}}}	{1:3}
    {0:{"spatialreference":{"wkid":4326},"coord":{"x":-125.75909851391168,"y":-83.36445481973186}}}	{1:4}
    {0:{"spatialreference":{"wkid":4326},"coord":{"x":-10.586696547910748,"y":-26.84047109087774}}}	{1:5}
    {0:{"spatialreference":{"wkid":4326},"coord":{"x":-41.19432437792534,"y":21.8966844948239}}}	{1:6}
    {0:{"spatialreference":{"wkid":4326},"coord":{"x":174.87478351390024,"y":4.045731094303122}}}	{1:7}
    {0:{"spatialreference":{"wkid":4326},"coord":{"x":130.762006312611,"y":-23.261282885155765}}}	{1:8}
    {0:{"spatialreference":{"wkid":4326},"coord":{"x":86.44882777211978,"y":-80.11749815286643}}}	{1:9}
    Time taken: 12.496 seconds
