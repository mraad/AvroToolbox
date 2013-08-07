AvroToolbox
===========

ArcGIS toolbox to store feature classes in HDFS in [Apache Avro](http://avro.apache.org) and [Parquet format](http://parquet.io)

A GIS generic feature geometry and attributes based on the [Avro specification](http://avro.apache.org/docs/current/spec.html) in AvroFeature.avsc file.

## Compiling the schema

    $ mvn avro:schema

## CDH3 Packaging

    $ mvn -Pcdh3 clean package

Copy the file target/AvroToolbox-1.0-SNAPSHOT.jar and the folder target/libs to C:\Program Files (x86)\ArcGIS\Desktop10.1\java\lib\ext

![Export To Avro](https://dl.dropboxusercontent.com/u/2193160/ExportToAvro.png "Export To Avro")

## CDH3 Running MapReduce job

    $ mvn -Pcdh3-job clean package
    $ hadoop jar target/AvroToolbox-1.0-SNAPSHOT-job.jar /user/mraad_admin/worldlabels.avro /user/mraad_admin/output

## CDH3 Viewing MapReduce result

    $ mvn -Pcdh3 clean package
    $ mvn -Pcdh3 exec:java -q -Dexec.mainClass=com.esri.AvroToJson -Dexec.args="hdfs://localhadoop:9000/user/mraad_admin/output/part-00000.avro"
