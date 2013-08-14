package com.esri;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * hadoop jar target/AvroToolbox-1.0-SNAPSHOT-job.jar /user/cloudera/points/points.avro /user/cloudera/output
 */
public final class ClusterFeatureJob extends Configured implements Tool
{
    private final static double XMIN = -180.0;
    private final static double YMIN = -90.0;

    private static final class FeatureMapper extends AvroMapper<
            AvroPointFeature, Pair<Long, Integer>>
    {
        @Override
        public void map(
                final AvroPointFeature feature,
                final AvroCollector<Pair<Long, Integer>> collector,
                final Reporter reporter)
                throws IOException
        {
            final AvroPoint avroPoint = feature.getGeometry();
            final AvroCoord coord = avroPoint.getCoord();
            final long x = (long) Math.floor(coord.getX() - XMIN);
            final long y = (long) Math.floor(coord.getY() - YMIN);
            final long g = (y << 32) | x;
            collector.collect(new Pair<Long, Integer>(g, 1));
        }
    }

    private static final class FeatureReducer extends AvroReducer<Long, Integer,
            Pair<Long, Integer>>
    {
        @Override
        public void reduce(
                final Long key,
                final Iterable<Integer> values,
                final AvroCollector<Pair<Long, Integer>> collector,
                final Reporter reporter)
                throws IOException
        {
            int sum = 0;
            for (final Integer value : values)
            {
                sum += value;
            }
            collector.collect(new Pair<Long, Integer>(key, sum));
        }
    }

    @Override
    public int run(final String[] args) throws Exception
    {
        if (args.length != 2)
        {
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }
        final JobConf jobConf = new JobConf(getConf(), ClusterFeatureJob.class);
        jobConf.setJobName(ClusterFeatureJob.class.getSimpleName());

        FileInputFormat.setInputPaths(jobConf, new Path(args[0]));

        final Path outputDir = new Path(args[1]);
        outputDir.getFileSystem(jobConf).delete(outputDir, true);
        FileOutputFormat.setOutputPath(jobConf, outputDir);

        AvroJob.setMapperClass(jobConf, FeatureMapper.class);
        AvroJob.setReducerClass(jobConf, FeatureReducer.class);

        AvroJob.setInputSchema(jobConf, AvroPointFeature.getClassSchema());
        AvroJob.setOutputSchema(jobConf,
                Pair.getPairSchema(Schema.create(Schema.Type.LONG),
                        Schema.create(Schema.Type.INT)));

        final Job job = new Job(jobConf);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception
    {
        System.exit(ToolRunner.run(new ClusterFeatureJob(), args));
    }

}
