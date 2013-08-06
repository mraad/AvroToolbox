package com.esri;

import com.esri.arcgis.geodatabase.FeatureClass;
import com.esri.arcgis.geodatabase.IFeature;
import com.esri.arcgis.geodatabase.IFeatureClass;
import com.esri.arcgis.geodatabase.IFeatureClassProxy;
import com.esri.arcgis.geodatabase.IFeatureCursor;
import com.esri.arcgis.geodatabase.IField;
import com.esri.arcgis.geodatabase.IFields;
import com.esri.arcgis.geodatabase.IGPMessages;
import com.esri.arcgis.geodatabase.IGPValue;
import com.esri.arcgis.geodatabase.esriFieldType;
import com.esri.arcgis.geometry.IEnvelope;
import com.esri.arcgis.geometry.IGeometry;
import com.esri.arcgis.geometry.ISpatialReference;
import com.esri.arcgis.geometry.ISpatialReferenceAuthority;
import com.esri.arcgis.geometry.esriSRGeoCSType;
import com.esri.arcgis.geoprocessing.IGPEnvironmentManager;
import com.esri.arcgis.interop.AutomationException;
import com.esri.arcgis.interop.Cleaner;
import com.esri.arcgis.system.Array;
import com.esri.arcgis.system.IArray;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;

/**
 */
public class ExportToAvroTool extends AbstractTool
{
    private static final long serialVersionUID = -6111893544409469534L;

    public static final String NAME = ExportToAvroTool.class.getSimpleName();

    @Override
    protected void doExecute(
            final IArray parameters,
            final IGPMessages messages,
            final IGPEnvironmentManager environmentManager) throws Exception
    {
        final IGPValue hadoopConfValue = gpUtilities.unpackGPValue(parameters.getElement(0));
        final IGPValue hadoopUserValue = gpUtilities.unpackGPValue(parameters.getElement(1));
        final IGPValue featureClassValue = gpUtilities.unpackGPValue(parameters.getElement(2));
        final IGPValue outputValue = gpUtilities.unpackGPValue(parameters.getElement(3));

        final UserGroupInformation ugi = UserGroupInformation.createRemoteUser(hadoopUserValue.getAsText());
        final int count = ugi.doAs(new PrivilegedExceptionAction<Integer>()
        {
            public Integer run() throws Exception
            {
                return doExport(hadoopConfValue, featureClassValue, outputValue);
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
            final IGPValue outputValue) throws Exception
    {
        int count = 0;
        final IFeatureClass[] featureClasses = new IFeatureClass[]{new IFeatureClassProxy()};
        gpUtilities.decodeFeatureLayer(featureClassValue, featureClasses, null);
        final FeatureClass featureClass = new FeatureClass(featureClasses[0]);

        final ISpatialReference spatialReference = featureClass.getSpatialReference();
        final int wkid;
        if (spatialReference instanceof ISpatialReferenceAuthority)
        {
            final ISpatialReferenceAuthority spatialReferenceAuthority = (ISpatialReferenceAuthority) spatialReference;
            wkid = spatialReferenceAuthority.getCode();
        }
        else
        {
            wkid = esriSRGeoCSType.esriSRGeoCS_WGS1984;
        }
        final int geometryType = featureClass.getFeatureType();

        final Configuration configuration = createConfiguration(hadoopPropValue.getAsText());
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
                final DatumWriter<AvroFeature> datumWriter = new SpecificDatumWriter<AvroFeature>(AvroFeature.class);
                final DataFileWriter<AvroFeature> dataFileWriter = new DataFileWriter<AvroFeature>(datumWriter);
                dataFileWriter.create(AvroFeature.getClassSchema(), fsDataOutputStream);
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
                                final Map<CharSequence, Object> attributes = new HashMap<CharSequence, Object>();
                                final int fieldCount = fields.getFieldCount();
                                for (int f = 0; f < fieldCount; f++)
                                {
                                    final IField field = fields.getField(f);
                                    final String name = field.getName();
                                    final Object value = feature.getValue(f);
                                    switch (field.getType())
                                    {
                                        case esriFieldType.esriFieldTypeDouble:
                                        case esriFieldType.esriFieldTypeString:
                                        case esriFieldType.esriFieldTypeSingle:
                                        case esriFieldType.esriFieldTypeInteger:
                                            attributes.put(name, value);
                                            break;
                                        case esriFieldType.esriFieldTypeSmallInteger:
                                            if (value instanceof Short)
                                            {
                                                attributes.put(name, ((Short) value).intValue());
                                            }
                                            else if (value instanceof Byte)
                                            {
                                                attributes.put(name, ((Byte) value).intValue());
                                            }
                                            break;
                                    }
                                }
                                final IGeometry shape = feature.getShape();
                                final IEnvelope envelope = shape.getEnvelope();
                                final AvroCoord coord = AvroCoord.newBuilder().
                                        setX((envelope.getXMin() + envelope.getXMax()) * 0.5).
                                        setY((envelope.getYMin() + envelope.getYMax()) * 0.5).
                                        build();
                                final AvroPoint point = AvroPoint.newBuilder().setCoord(coord).build();
                                final AvroFeature avroFeature = AvroFeature.newBuilder().setGeometry(point).setAttributes(attributes).build();
                                dataFileWriter.append(avroFeature);
                                feature = cursor.nextFeature();
                                count++;
                            }
                        }
                        finally
                        {
                            Cleaner.release(feature);
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


    @Override
    public IArray getParameterInfo() throws IOException, AutomationException
    {
        final String username = System.getProperty("user.name");
        final String userhome = System.getProperty("user.home") + File.separator;

        final IArray parameters = new Array();

        addParamFile(parameters, "Hadoop properties file", "in_hadoop_prop", userhome + "hadoop.properties");
        addParamString(parameters, "Hadoop user", "in_hadoop_user", username);
        addParamFeatureLayer(parameters, "Input features", "in_features");
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
