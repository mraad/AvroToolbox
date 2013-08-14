package com.esri;

import com.esri.arcgis.datasourcesfile.DEFile;
import com.esri.arcgis.datasourcesfile.DEFileType;
import com.esri.arcgis.datasourcesfile.DELayerType;
import com.esri.arcgis.geodatabase.DEFeatureClassType;
import com.esri.arcgis.geodatabase.DETable;
import com.esri.arcgis.geodatabase.DETableType;
import com.esri.arcgis.geodatabase.FeatureClass;
import com.esri.arcgis.geodatabase.IGPMessages;
import com.esri.arcgis.geometry.ISpatialReference;
import com.esri.arcgis.geometry.ISpatialReferenceAuthority;
import com.esri.arcgis.geometry.esriSRGeoCSType;
import com.esri.arcgis.geoprocessing.BaseGeoprocessingTool;
import com.esri.arcgis.geoprocessing.GPCompositeDataType;
import com.esri.arcgis.geoprocessing.GPFeatureLayer;
import com.esri.arcgis.geoprocessing.GPFeatureLayerType;
import com.esri.arcgis.geoprocessing.GPFeatureRecordSetLayerType;
import com.esri.arcgis.geoprocessing.GPLayerType;
import com.esri.arcgis.geoprocessing.GPParameter;
import com.esri.arcgis.geoprocessing.GPString;
import com.esri.arcgis.geoprocessing.GPStringType;
import com.esri.arcgis.geoprocessing.GPTableSchema;
import com.esri.arcgis.geoprocessing.IGPEnvironmentManager;
import com.esri.arcgis.geoprocessing.esriGPParameterDirection;
import com.esri.arcgis.geoprocessing.esriGPParameterType;
import com.esri.arcgis.interop.AutomationException;
import com.esri.arcgis.interop.Cleaner;
import com.esri.arcgis.system.IArray;
import com.esri.arcgis.system.IName;
import com.esri.arcgis.system.ITrackCancel;
import org.apache.hadoop.conf.Configuration;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

/**
 */
public abstract class AbstractTool extends BaseGeoprocessingTool
{
    private final static AvroToolbox FACTORY = new AvroToolbox();

    @Override
    public IName getFullName() throws IOException, AutomationException
    {
        return (IName) FACTORY.getFunctionName(getName());
    }

    @Override
    public boolean isLicensed() throws IOException, AutomationException
    {
        return true;
    }

    @Override
    public void updateMessages(
            final IArray parameters,
            final IGPEnvironmentManager environmentManager,
            final IGPMessages messages)
    {
    }

    @Override
    public void updateParameters(
            final IArray parameters,
            final IGPEnvironmentManager environmentManager)
    {
    }

    @Override
    public String getMetadataFile() throws IOException, AutomationException
    {
        return null;
    }

    @Override
    public void execute(
            final IArray parameters,
            final ITrackCancel trackCancel,
            final IGPEnvironmentManager environmentManager,
            final IGPMessages messages) throws IOException, AutomationException
    {
        try
        {
            Thread.currentThread().setContextClassLoader(ClassLoader.getSystemClassLoader());

            doExecute(parameters, messages, environmentManager);
        }
        catch (Throwable t)
        {
            messages.addAbort(t.toString());
            for (final StackTraceElement stackTraceElement : t.getStackTrace())
            {
                messages.addAbort(stackTraceElement.toString());
            }
        }
    }

    protected void addParamString(
            final IArray parameters,
            final String displayName,
            final String name,
            final String value) throws IOException
    {
        final GPParameter parameter = new GPParameter();
        parameter.setDirection(esriGPParameterDirection.esriGPParameterDirectionInput);
        parameter.setDisplayName(displayName);
        parameter.setName(name);
        parameter.setParameterType(esriGPParameterType.esriGPParameterTypeRequired);
        parameter.setDataTypeByRef(new GPStringType());
        final GPString gpString = new GPString();
        gpString.setValue(value);
        parameter.setValueByRef(gpString);
        parameters.add(parameter);
    }

    protected void addParamFile(
            final IArray parameters,
            final String displayName,
            final String name,
            final String catalogPath
    ) throws IOException
    {
        final GPParameter parameter = new GPParameter();
        parameter.setDirection(esriGPParameterDirection.esriGPParameterDirectionInput);
        parameter.setDisplayName(displayName);
        parameter.setName(name);
        parameter.setParameterType(esriGPParameterType.esriGPParameterTypeRequired);
        parameter.setDataTypeByRef(new DEFileType());
        final DEFile file = new DEFile();
        file.setCatalogPath(catalogPath);
        parameter.setValueByRef(file);
        parameters.add(parameter);
    }

    protected void addParamFeatureLayer(
            final IArray parameters,
            final String displayName,
            final String name) throws IOException
    {
        final GPCompositeDataType compositeDataType = new GPCompositeDataType();
        compositeDataType.addDataType(new GPFeatureLayerType());
        compositeDataType.addDataType(new DEFeatureClassType());
        compositeDataType.addDataType(new GPLayerType());
        compositeDataType.addDataType(new DELayerType());
        compositeDataType.addDataType(new GPFeatureRecordSetLayerType());

        final GPParameter parameter = new GPParameter();
        parameter.setDirection(esriGPParameterDirection.esriGPParameterDirectionInput);
        parameter.setDisplayName(displayName);
        parameter.setName(name);
        parameter.setParameterType(esriGPParameterType.esriGPParameterTypeRequired);
        parameter.setDataTypeByRef(compositeDataType);
        parameter.setValueByRef(new GPFeatureLayer());
        parameters.add(parameter);
    }

    protected void addParamTable(
            final IArray parameters,
            final String displayName,
            final String name,
            final String value) throws IOException
    {
        final GPTableSchema tableSchema = new GPTableSchema();
        tableSchema.setCloneDependency(true);

        final GPParameter parameterOut = new GPParameter();
        parameterOut.setDirection(esriGPParameterDirection.esriGPParameterDirectionOutput);
        parameterOut.setDisplayName(displayName);
        parameterOut.setName(name);
        parameterOut.setParameterType(esriGPParameterType.esriGPParameterTypeRequired);
        parameterOut.setDataTypeByRef(new DETableType());
        final DETable table = new DETable();
        table.setAsText(value);
        parameterOut.setValueByRef(table);
        parameterOut.setSchemaByRef(tableSchema);

        parameters.add(parameterOut);
    }

    protected void loadProperties(
            final Configuration configuration,
            final String propertiesPath) throws IOException
    {
        final Properties properties = new Properties();
        final InputStream inputStream = new FileInputStream(propertiesPath);
        try
        {
            properties.load(inputStream);
        }
        finally
        {
            inputStream.close();
        }
        for (final Map.Entry<Object, Object> entry : properties.entrySet())
        {
            configuration.set(entry.getKey().toString(), entry.getValue().toString());
        }
    }

    protected abstract void doExecute(
            final IArray parameters,
            final IGPMessages messages,
            final IGPEnvironmentManager environmentManager) throws Throwable;

    protected Configuration createConfiguration(
            final String propertiesPath) throws IOException
    {
        final Configuration configuration = new Configuration();
        configuration.setClassLoader(ClassLoader.getSystemClassLoader());
        loadProperties(configuration, propertiesPath);
        return configuration;
    }

    protected int getWkid(final FeatureClass featureClass) throws IOException
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
}
