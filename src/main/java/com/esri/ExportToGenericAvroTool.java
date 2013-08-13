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
import com.esri.arcgis.geometry.ISpatialReference;
import com.esri.arcgis.geometry.ISpatialReferenceAuthority;
import com.esri.arcgis.geometry.Point;
import com.esri.arcgis.geometry.Polygon;
import com.esri.arcgis.geometry.Polyline;
import com.esri.arcgis.geometry.esriSRGeoCSType;
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
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
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

    private Configuration createConfiguration(
            final String propertiesPath) throws IOException
    {
        final Configuration configuration = new Configuration();
        configuration.setClassLoader(ClassLoader.getSystemClassLoader());
        loadProperties(configuration, propertiesPath);
        return configuration;
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

        // final String shapeFieldName = featureClass.getShapeFieldName();

        final ISpatialReference spatialReference = featureClass.getSpatialReference();
        final int wkid;
        if (spatialReference instanceof ISpatialReferenceAuthority)
        {
            final ISpatialReferenceAuthority spatialReferenceAuthority = (ISpatialReferenceAuthority) spatialReference;
            final int code = spatialReferenceAuthority.getCode();
            wkid = code == 0 ? esriSRGeoCSType.esriSRGeoCS_WGS1984 : code;
        }
        else
        {
            wkid = esriSRGeoCSType.esriSRGeoCS_WGS1984;
        }

        final Configuration configuration = createConfiguration(hadoopPropValue.getAsText());

        final Schema schema = parseSchema(schemaValue.getAsText(), configuration);

        final Path path = new Path(outputValue.getAsText());
        final FileSystem fileSystem = path.getFileSystem(configuration);
        try
        {
            if (fileSystem.exists(path))
            {
                fileSystem.delete(path, true);
            }
            final FSDataOutputStream fsDataOutputStream = fileSystem.create(path);
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
                IOUtils.closeStream(fsDataOutputStream);
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
        return null;
    }

    private GenericRecord buildRecord(
            final Schema schema,
            final int wkid,
            final IFields fields,
            final IFeature feature,
            final Polyline shape)
    {
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
        final String username = System.getProperty("user.name");
        final String userhome = System.getProperty("user.home") + File.separator;

        final IArray parameters = new Array();

        addParamFile(parameters, "Hadoop properties file", "in_hadoop_prop", userhome + "hadoop.properties");
        addParamString(parameters, "Hadoop user", "in_hadoop_user", username);
        addParamFeatureLayer(parameters, "Input feature class", "in_features");
        addParamString(parameters, "Input schema", "in_schema", "/user/" + username + "/worldlabels.avsc");
        addParamString(parameters, "Remote output path", "in_output_path", "/user/" + username + "/worldlabels.avro");

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
