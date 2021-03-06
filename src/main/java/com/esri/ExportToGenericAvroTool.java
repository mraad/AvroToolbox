package com.esri;

import com.esri.arcgis.geodatabase.FeatureClass;
import com.esri.arcgis.geodatabase.IFeature;
import com.esri.arcgis.geodatabase.IFeatureClass;
import com.esri.arcgis.geodatabase.IFeatureClassProxy;
import com.esri.arcgis.geodatabase.IFeatureCursor;
import com.esri.arcgis.geodatabase.IFields;
import com.esri.arcgis.geodatabase.IGPMessages;
import com.esri.arcgis.geodatabase.IGPValue;
import com.esri.arcgis.geometry.IGeometry;
import com.esri.arcgis.geometry.Point;
import com.esri.arcgis.geometry.Polygon;
import com.esri.arcgis.geometry.Polyline;
import com.esri.arcgis.geoprocessing.IGPEnvironmentManager;
import com.esri.arcgis.interop.AutomationException;
import com.esri.arcgis.interop.Cleaner;
import com.esri.arcgis.system.Array;
import com.esri.arcgis.system.IArray;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

/**
 */
public final class ExportToGenericAvroTool extends AbstractTool
{
    private static final long serialVersionUID = 6206775842012442573L;

    public static final String NAME = ExportToGenericAvroTool.class.getSimpleName();

    @Override
    protected void doExecute(
            final IArray parameters,
            final IGPMessages messages,
            final IGPEnvironmentManager environmentManager) throws Exception
    {
        final IGPValue hadoopConfValue = gpUtilities.unpackGPValue(parameters.getElement(0));
        final IGPValue hadoopUserValue = gpUtilities.unpackGPValue(parameters.getElement(1));
        final IGPValue featureClassValue = gpUtilities.unpackGPValue(parameters.getElement(2));
        final IGPValue schemaValue = gpUtilities.unpackGPValue(parameters.getElement(3));
        final IGPValue outputValue = gpUtilities.unpackGPValue(parameters.getElement(4));

        final UserGroupInformation ugi = UserGroupInformation.createRemoteUser(hadoopUserValue.getAsText());
        final int count = ugi.doAs(new PrivilegedExceptionAction<Integer>()
        {
            public Integer run() throws Exception
            {
                return doExport(hadoopConfValue, featureClassValue, schemaValue, outputValue);
            }
        });
        messages.addMessage(String.format("Exported %d features.", count));
    }

    private int doExport(
            final IGPValue hadoopPropValue,
            final IGPValue featureClassValue,
            final IGPValue schemaValue,
            final IGPValue outputValue) throws Exception
    {
        int count = 0;
        final IFeatureClass[] featureClasses = new IFeatureClass[]{new IFeatureClassProxy()};
        gpUtilities.decodeFeatureLayer(featureClassValue, featureClasses, null);
        final FeatureClass featureClass = new FeatureClass(featureClasses[0]);

        final int wkid = getWkid(featureClass);

        final Configuration configuration = createConfiguration(hadoopPropValue.getAsText());

        final Schema schema = parseSchema(schemaValue.getAsText(), configuration);

        final Path path = new Path(outputValue.getAsText());
        final FileSystem fileSystem = path.getFileSystem(configuration);
        try
        {
            final FSDataOutputStream fsDataOutputStream = fileSystem.create(path, true);
            try
            {
                final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
                final DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
                dataFileWriter.create(schema, fsDataOutputStream);
                try
                {
                    final IFeatureCursor cursor = featureClass.search(null, false);
                    try
                    {
                        final IFields fields = cursor.getFields();
                        IFeature feature = cursor.nextFeature();
                        try
                        {
                            while (feature != null)
                            {
                                final IGeometry shape = feature.getShape();
                                if (shape instanceof Point)
                                {
                                    dataFileWriter.append(buildRecord(schema, wkid, fields, feature, (Point) shape));
                                    count++;
                                }
                                else if (shape instanceof Polyline)
                                {
                                    dataFileWriter.append(buildRecord(schema, wkid, fields, feature, (Polyline) shape));
                                    count++;
                                }
                                else if (shape instanceof Polygon)
                                {
                                    dataFileWriter.append(buildRecord(schema, wkid, fields, feature, (Polygon) shape));
                                    count++;
                                }
                                feature = cursor.nextFeature();
                            }
                        }
                        finally
                        {
                            Cleaner.release(fields);
                        }
                    }
                    finally
                    {
                        Cleaner.release(cursor);
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
        Cleaner.release(featureClass);
        return count;
    }

    private GenericRecord buildRecord(
            final Schema schema,
            final int wkid,
            final IFields fields,
            final IFeature feature,
            final Polygon shape)
    {
        // TODO
        return null;
    }

    private GenericRecord buildRecord(
            final Schema schema,
            final int wkid,
            final IFields fields,
            final IFeature feature,
            final Polyline shape)
    {
        // TODO
        return null;
    }

    private GenericRecord buildRecord(
            final Schema schema,
            final int wkid,
            final IFields fields,
            final IFeature feature,
            final Point shape) throws IOException
    {
        final GenericRecord genericRecord = new GenericData.Record(schema);
        for (final Schema.Field schemaField : schema.getFields())
        {
            final int index = fields.findField(schemaField.name());
            if (index > -1)
            {
                genericRecord.put(schemaField.name(), feature.getValue(index));
            }
        }

        final Schema.Field geometryField = schema.getField("geometry");
        final Schema geometrySchema = geometryField.schema();
        final GenericRecord geometry = new GenericData.Record(geometrySchema);
        genericRecord.put("geometry", geometry);

        final Schema.Field spatialReferenceField = geometrySchema.getField("spatialReference");
        final GenericRecord spatialReference = new GenericData.Record(spatialReferenceField.schema());
        spatialReference.put("wkid", wkid);
        geometry.put("spatialReference", spatialReference);

        final Schema.Field coordField = geometrySchema.getField("coord");
        final GenericRecord coord = new GenericData.Record(coordField.schema());
        coord.put("x", shape.getX());
        coord.put("y", shape.getY());
        geometry.put("coord", coord);

        return genericRecord;
    }

    private Schema parseSchema(
            final String location,
            final Configuration configuration) throws IOException
    {
        final Schema schema;
        final Path path = new Path(location);
        final FileSystem fileSystem = path.getFileSystem(configuration);
        try
        {
            final FSDataInputStream dataInputStream = fileSystem.open(path);
            try
            {
                schema = new Schema.Parser().parse(dataInputStream);
            }
            finally
            {
                dataInputStream.close();
            }
        }
        finally
        {
            fileSystem.close();
        }
        return schema;
    }

    @Override
    public IArray getParameterInfo() throws IOException, AutomationException
    {
        final String username = "cloudera"; // System.getProperty("user.name");

        final IArray parameters = new Array();

        addParamHadoopProperties(parameters);
        addParamHadoopUser(parameters, username);
        addParamFeatureLayer(parameters);
        addParamString(parameters, "Input schema", "in_schema", "/user/" + username + "/features.avsc");
        addParamString(parameters, "Remote output path", "in_output_path", "/user/" + username + "/features.avro");

        return parameters;
    }

    @Override
    public String getName() throws IOException, AutomationException
    {
        return NAME;
    }

    @Override
    public String getDisplayName() throws IOException, AutomationException
    {
        return NAME;
    }
}
