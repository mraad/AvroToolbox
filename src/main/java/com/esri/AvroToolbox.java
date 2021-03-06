package com.esri;

import com.esri.arcgis.geodatabase.IEnumGPName;
import com.esri.arcgis.geodatabase.IGPName;
import com.esri.arcgis.geoprocessing.EnumGPName;
import com.esri.arcgis.geoprocessing.GPFunctionName;
import com.esri.arcgis.geoprocessing.IEnumGPEnvironment;
import com.esri.arcgis.geoprocessing.IGPFunction;
import com.esri.arcgis.geoprocessing.IGPFunctionFactory;
import com.esri.arcgis.interop.AutomationException;
import com.esri.arcgis.interop.extn.ArcGISCategories;
import com.esri.arcgis.interop.extn.ArcGISExtension;
import com.esri.arcgis.system.IUID;
import com.esri.arcgis.system.UID;

import java.io.IOException;
import java.util.UUID;

/**
 */
@ArcGISExtension(categories = {ArcGISCategories.GPFunctionFactories})
public final class AvroToolbox implements IGPFunctionFactory
{
    private static final long serialVersionUID = 2385676665599961990L;

    private static final String NAME = AvroToolbox.class.getSimpleName();

    public IUID getCLSID() throws IOException, AutomationException
    {
        final UID uid = new UID();
        uid.setValue("{" + UUID.nameUUIDFromBytes(this.getClass().getName().getBytes()) + "}");
        return uid;
    }

    public String getName() throws IOException, AutomationException
    {
        return NAME;
    }

    public String getAlias() throws IOException, AutomationException
    {
        return NAME;
    }

    public IGPFunction getFunction(final String s) throws IOException, AutomationException
    {
        if (ExportToGenericAvroTool.NAME.equalsIgnoreCase(s))
        {
            return new ExportToGenericAvroTool();
        }
        if (ExportToAvroParquetTool.NAME.equalsIgnoreCase(s))
        {
            return new ExportToAvroParquetTool();
        }
        if (ExportToAvroTool.NAME.equalsIgnoreCase(s))
        {
            return new ExportToAvroTool();
        }
        if (SchemaTool.NAME.equalsIgnoreCase(s))
        {
            return new SchemaTool();
        }
        return null;
    }

    public IGPName getFunctionName(final String s) throws IOException, AutomationException
    {
        if (ExportToGenericAvroTool.NAME.equalsIgnoreCase(s))
        {
            final GPFunctionName functionName = new GPFunctionName();
            functionName.setCategory(ExportToGenericAvroTool.NAME);
            functionName.setDescription(ExportToGenericAvroTool.NAME);
            functionName.setDisplayName(ExportToGenericAvroTool.NAME);
            functionName.setName(ExportToGenericAvroTool.NAME);
            functionName.setFactoryByRef(this);
            return functionName;
        }
        if (ExportToAvroParquetTool.NAME.equalsIgnoreCase(s))
        {
            final GPFunctionName functionName = new GPFunctionName();
            functionName.setCategory(ExportToAvroParquetTool.NAME);
            functionName.setDescription(ExportToAvroParquetTool.NAME);
            functionName.setDisplayName(ExportToAvroParquetTool.NAME);
            functionName.setName(ExportToAvroParquetTool.NAME);
            functionName.setFactoryByRef(this);
            return functionName;
        }
        if (ExportToAvroTool.NAME.equalsIgnoreCase(s))
        {
            final GPFunctionName functionName = new GPFunctionName();
            functionName.setCategory(ExportToAvroTool.NAME);
            functionName.setDescription(ExportToAvroTool.NAME);
            functionName.setDisplayName(ExportToAvroTool.NAME);
            functionName.setName(ExportToAvroTool.NAME);
            functionName.setFactoryByRef(this);
            return functionName;
        }
        if (SchemaTool.NAME.equalsIgnoreCase(s))
        {
            final GPFunctionName functionName = new GPFunctionName();
            functionName.setCategory(SchemaTool.NAME);
            functionName.setDescription(SchemaTool.NAME);
            functionName.setDisplayName(SchemaTool.NAME);
            functionName.setName(SchemaTool.NAME);
            functionName.setFactoryByRef(this);
            return functionName;
        }
        return null;
    }

    public IEnumGPName getFunctionNames() throws IOException, AutomationException
    {
        final EnumGPName nameArray = new EnumGPName();
        nameArray.add(getFunctionName(ExportToGenericAvroTool.NAME));
        nameArray.add(getFunctionName(ExportToAvroParquetTool.NAME));
        nameArray.add(getFunctionName(ExportToAvroTool.NAME));
        nameArray.add(getFunctionName(SchemaTool.NAME));
        return nameArray;
    }

    public IEnumGPEnvironment getFunctionEnvironments() throws IOException, AutomationException
    {
        return null;
    }
}