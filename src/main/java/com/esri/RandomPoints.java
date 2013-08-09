package com.esri;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * mvn -Pcdh4 exec:java -q -Dexec.mainClass=com.esri.RandomPoints -Dexec.args="hdfs://localhost:8020/user/cloudera/point/points.avro"
 */
public class RandomPoints
{
    public static void main(final String[] args) throws IOException
    {
        if (args.length == 0)
        {
            System.err.println("Missing output path argument !");
            System.exit(-1);
        }
        final Configuration configuration = new Configuration();
        final Path path = new Path(args[0]);
        final FileSystem fileSystem = path.getFileSystem(configuration);
        try
        {
            fileSystem.delete(path, true);
            final FSDataOutputStream fsDataOutputStream = fileSystem.create(path);
            try
            {
                final DatumWriter<AvroPointFeature> datumWriter = new SpecificDatumWriter<AvroPointFeature>(AvroPointFeature.class);
                final DataFileWriter<AvroPointFeature> dataFileWriter = new DataFileWriter<AvroPointFeature>(datumWriter);
                try
                {
                    dataFileWriter.create(AvroPointFeature.getClassSchema(), fsDataOutputStream);
                    final Map<CharSequence, Object> map = new HashMap<CharSequence, Object>();
                    final AvroSpatialReference spatialReference = AvroSpatialReference.newBuilder().
                            setWkid(4326).
                            build();
                    for (int i = 0; i < 100; i++)
                    {
                        map.put("id", i);
                        final double x = -180.0 + 360.0 * Math.random();
                        final double y = -90.0 + 180 * Math.random();
                        final AvroCoord avroCoord = AvroCoord.newBuilder().
                                setX(x).setY(y).build();
                        final AvroPoint avroPoint = AvroPoint.newBuilder().
                                setCoord(avroCoord).setSpatialReference(spatialReference).
                                build();
                        final AvroPointFeature avroFeature = AvroPointFeature.newBuilder().
                                setGeometry(avroPoint).
                                setAttributes(map).build();
                        dataFileWriter.append(avroFeature);
                    }
                }
                finally
                {
                    dataFileWriter.close();
                }
            }
            finally
            {
                fsDataOutputStream.close();
            }
        }
        finally
        {
            fileSystem.close();
        }
    }
}
