package com.esri;

import com.esri.arcgis.geodatabase.FeatureClass;
import com.esri.arcgis.geodatabase.IFeatureClass;
import com.esri.arcgis.geodatabase.IFeatureClassProxy;
import com.esri.arcgis.geodatabase.IField;
import com.esri.arcgis.geodatabase.IFields;
import com.esri.arcgis.geodatabase.IGPMessages;
import com.esri.arcgis.geodatabase.IGPValue;
import com.esri.arcgis.geodatabase.esriFieldType;
import com.esri.arcgis.geometry.ISpatialReference;
import com.esri.arcgis.geometry.ISpatialReferenceAuthority;
import com.esri.arcgis.geometry.esriSRGeoCSType;
import com.esri.arcgis.geometry.esriShapeType;
import com.esri.arcgis.geoprocessing.IGPEnvironmentManager;
import com.esri.arcgis.interop.AutomationException;
import com.esri.arcgis.interop.Cleaner;
import com.esri.arcgis.system.Array;
import com.esri.arcgis.system.IArray;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;

/**
 */
public final class SchemaTool extends AbstractTool
{
    private static final long serialVersionUID = 7200724910600739778L;

    public static final String NAME = SchemaTool.class.getSimpleName();

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
        ugi.doAs(new PrivilegedExceptionAction<Void>()
        {
            public Void run() throws Exception
            {
                doExport(hadoopConfValue, featureClassValue, outputValue, messages);
                return null;
            }
        });
    }

    private Configuration createConfiguration(
            final String propertiesPath) throws IOException
    {
        final Configuration configuration = new Configuration();
        configuration.setClassLoader(ClassLoader.getSystemClassLoader());
        loadProperties(configuration, propertiesPath);
        return configuration;
    }

    private void doExport(
            final IGPValue hadoopPropValue,
            final IGPValue featureClassValue,
            final IGPValue outputValue,
            final IGPMessages messages) throws Exception
    {
        final IFeatureClass[] featureClasses = new IFeatureClass[]{new IFeatureClassProxy()};
        gpUtilities.decodeFeatureLayer(featureClassValue, featureClasses, null);
        final FeatureClass featureClass = new FeatureClass(featureClasses[0]);
        try
        {
            final Configuration configuration = createConfiguration(hadoopPropValue.getAsText());
            final String namespace = configuration.get("schema.namespace", "com.esri");
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
                    doSchema(fsDataOutputStream, namespace, featureClass, messages);
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
        }
        finally
        {
            Cleaner.release(featureClass);
        }
    }

    private void doSchema(
            final FSDataOutputStream fsDataOutputStream,
            final String namespace,
            final FeatureClass featureClass,
            final IGPMessages messages) throws IOException
    {
        final int wkid = getWkid(featureClass);
        final JsonFactory jsonFactory = new JsonFactory();
        final JsonGenerator g = jsonFactory.createJsonGenerator(fsDataOutputStream);
        try
        {
            g.writeStartObject();
            g.writeStringField("type", "record");
            g.writeStringField("namespace", namespace);
            g.writeStringField("name", featureClass.getName());
            g.writeArrayFieldStart("fields");
            writeShape(g, namespace, featureClass.getShapeType(), wkid, messages);
            writeFields(g, featureClass, messages);
            g.writeEndArray();
            g.writeEndObject();
        }
        finally
        {
            g.close();
        }
    }

    private int getWkid(final FeatureClass featureClass) throws IOException
    {
        final int wkid;
        final ISpatialReference spatialReference = featureClass.getSpatialReference();
        try
        {
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
        }
        finally
        {
            Cleaner.release(spatialReference);
        }
        return wkid;
    }

    private void writeFields(
            final JsonGenerator g,
            final FeatureClass featureClass,
            final IGPMessages messages) throws IOException
    {
        final IFields fields = featureClass.getFields();
        try
        {
            final int count = fields.getFieldCount();
            for (int c = 0; c < count; c++)
            {
                final IField field = fields.getField(c);
                messages.addMessage(String.format("%s %d %d",
                        field.getName(), field.getType(), field.getLength()));
                switch (field.getType())
                {
                    case esriFieldType.esriFieldTypeString:
                        writeField(g, field, "string");
                        break;
                    case esriFieldType.esriFieldTypeDouble:
                        writeField(g, field, "double");
                        break;
                    case esriFieldType.esriFieldTypeSingle:
                        writeField(g, field, "float");
                        break;
                    case esriFieldType.esriFieldTypeInteger:
                        writeField(g, field, "int");
                        break;
                    case esriFieldType.esriFieldTypeSmallInteger:
                        writeField(g, field, "int");
                        break;
                }
            }
        }
        finally
        {
            Cleaner.release(fields);
        }
    }

    private void writeField(
            final JsonGenerator g,
            final IField field,
            final String type) throws IOException
    {
        g.writeStartObject();
        g.writeStringField("name", field.getName());
        g.writeStringField("type", type);
        final Object defValue = field.getDefaultValue();
        if (defValue instanceof String)
        {
            g.writeStringField("default", defValue.toString());
        }
        else if (defValue instanceof Float)
        {
            g.writeNumberField("default", (Float) defValue);
        }
        else if (defValue instanceof Double)
        {
            g.writeNumberField("default", (Double) defValue);
        }
        else if (defValue instanceof Short)
        {
            g.writeNumberField("default", (Short) defValue);
        }
        else if (defValue instanceof Integer)
        {
            g.writeNumberField("default", (Integer) defValue);
        }
        else if (defValue instanceof Long)
        {
            g.writeNumberField("default", (Long) defValue);
        }
        g.writeEndObject();
    }

    private void writeShape(
            final JsonGenerator g,
            final String namespace,
            final int shapeType,
            final int wkid,
            final IGPMessages messages) throws IOException
    {
        messages.addMessage(String.format("shapeType = %d, wkid = %d", shapeType, wkid));
        switch (shapeType)
        {
            case esriShapeType.esriShapePoint:
                writePoint(g, namespace, wkid);
                break;
            case esriShapeType.esriShapePolyline:
                writePolyline(g, namespace, wkid);
                break;
            case 4:
            case esriShapeType.esriShapePolygon:
                writePolygon(g, namespace, wkid);
                break;
        }
    }

    private void writePolygon(
            final JsonGenerator g,
            final String namespace,
            final int wkid) throws IOException
    {
        g.writeStartObject();
        g.writeStringField("name", "geometry");
        // g.writeStringField("default", "null");
        writeRecordStart(g, namespace, "AvroPolygon");
        writeSpatialReference(g, namespace, wkid);
        writeRingPath(g, namespace, "rings");
        g.writeEndArray();
        g.writeEndObject();
        g.writeEndObject();
    }

    private void writePolyline(
            final JsonGenerator g,
            final String namespace,
            final int wkid) throws IOException
    {
        g.writeStartObject();
        g.writeStringField("name", "geometry");
        // g.writeStringField("default", "null");
        writeRecordStart(g, namespace, "AvroPolyline");
        writeSpatialReference(g, namespace, wkid);
        writeRingPath(g, namespace, "paths");
        g.writeEndArray();
        g.writeEndObject();
        g.writeEndObject();
    }

    private void writeRingPath(
            final JsonGenerator g,
            final String namespace,
            final String name) throws IOException
    {
        g.writeStartObject();
        {
            g.writeStringField("name", name);
            // g.writeStringField("default", "null");
            g.writeObjectFieldStart("type");
            {
                g.writeStringField("type", "array");
                g.writeObjectFieldStart("items");
                {
                    g.writeStringField("type", "array");
                    writeCoord(g, namespace);
                }
                g.writeEndObject(); // items
            }
            g.writeEndObject(); // type
        }
        g.writeEndObject();
    }

    private void writePoint(
            final JsonGenerator g,
            final String namespace,
            final int wkid) throws IOException
    {
        g.writeStartObject();
        {
            g.writeStringField("name", "geometry");
            // g.writeStringField("default", "null");
            g.writeObjectFieldStart("type");
            {
                g.writeStringField("type", "record");
                g.writeStringField("namespace", namespace);
                g.writeStringField("name", "AvroPoint");
                g.writeArrayFieldStart("fields");
                {
                    writeSpatialReference(g, namespace, wkid);
                    g.writeStartObject();
                    {
                        g.writeStringField("name", "coord");
                        // g.writeStringField("default", "null");
                        g.writeObjectFieldStart("type");
                        writeAvroCoord(g, namespace);
                        g.writeEndObject();
                    }
                    g.writeEndObject();
                }
                g.writeEndArray();
            }
            g.writeEndObject();
        }
        g.writeEndObject();
    }

    private void writeRecordStart(
            final JsonGenerator g,
            final String namespace,
            final String name) throws IOException
    {
        g.writeObjectFieldStart("type");
        g.writeStringField("type", "record");
        g.writeStringField("namespace", namespace);
        g.writeStringField("name", name);
        g.writeArrayFieldStart("fields");
    }

    private void writeRecordEnd(final JsonGenerator g) throws IOException
    {
        g.writeEndArray();
        g.writeEndObject();
    }

    private void writeCoord(
            final JsonGenerator g,
            final String namespace) throws IOException
    {
        g.writeStartObject();
        writeAvroCoord(g, namespace);
        g.writeEndObject();
    }

    private void writeAvroCoord(
            final JsonGenerator g,
            final String namespace) throws IOException
    {
        g.writeStringField("type", "record");
        g.writeStringField("namespace", namespace);
        g.writeStringField("name", "AvroCoord");
        g.writeArrayFieldStart("fields");
        {
            writeField(g, "x", "double", 0.0);
            writeField(g, "y", "double", 0.0);
        }
        g.writeEndArray();
    }

    private void writeSpatialReference(
            final JsonGenerator g,
            final String namespace,
            final int wkid
    ) throws IOException
    {
        g.writeStartObject();
        g.writeStringField("name", "spatialReference");
        // g.writeStringField("default", "null");
        writeRecordStart(g, namespace, "AvroSpatialReference");
        writeField(g, "wkid", "int", wkid);
        writeRecordEnd(g);
        g.writeEndObject();
    }

    private void writeField(
            final JsonGenerator g,
            final String name,
            final String type,
            final int defValue) throws IOException
    {
        g.writeStartObject();
        g.writeStringField("name", name);
        g.writeStringField("type", type);
        g.writeNumberField("default", defValue);
        g.writeEndObject();
    }

    private void writeField(
            final JsonGenerator g,
            final String name,
            final String type,
            final double defValue) throws IOException
    {
        g.writeStartObject();
        g.writeStringField("name", name);
        g.writeStringField("type", type);
        g.writeNumberField("default", defValue);
        g.writeEndObject();
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
        addParamString(parameters, "Schema output path", "in_output_path", "/user/" + username + "/features.avsc");

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
