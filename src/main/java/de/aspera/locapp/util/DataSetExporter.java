package de.aspera.locapp.util;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.dbunit.database.AmbiguousTableNameException;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.xml.FlatXmlDataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unitils.core.Unitils;







/**
 * Use this to export data from database to a flat XML data set..
 * 
 * @author Petr Stastny
 * 
 */
public class DataSetExporter {

    private static final Logger logger   = LoggerFactory.getLogger(DataSetExporter.class);

    public final static DataSetExporter   INSTANCE = new DataSetExporter();

    public void exportDataSet(String path, Class<?> callerClass, String testMethod, TableDescriptor... descriptors) {
        exportDataSetInternal(this.generateDataSetFileName(path, callerClass, testMethod), descriptors);
    }

    public void exportExpectedDataSet(String path, Class<?> callerClass, String testMethod,
            TableDescriptor... descriptors) {
        exportDataSetInternal(this.generateExpectedDataSetFileName(path, callerClass, testMethod), descriptors);
    }

    protected String generateDataSetFileName(String path, Class<?> callerClass, String testMethod) {
        StringBuilder builder = new StringBuilder();

        builder.append(path).append("/").append(callerClass.getSimpleName()).append(".").append(testMethod)
                .append(".xml.sample");

        String generatedFileName = builder.toString();

        logger.debug("generated file name: {}", generatedFileName);
        return generatedFileName;
    }

    protected String generateExpectedDataSetFileName(String path, Class<?> callerClass, String testMethod) {
        StringBuilder builder = new StringBuilder();

        builder.append(path).append("/").append(callerClass.getSimpleName()).append(".").append(testMethod)
                .append("-result.xml.sample");

        String generatedFileName = builder.toString();

        logger.debug("generated file name: {}", generatedFileName);
        return generatedFileName;
    }

    private IDatabaseConnection connection;

    /** 
     * Reuse the connection where possible.
     * 
     */
    public IDatabaseConnection getConnection() {
        return connection;
    }

    protected void exportDataSetInternal(String dataSetFileName, TableDescriptor... descriptors) {

        IDatabaseConnection connection = this.getConnection();

        DbUnitModule dbUnitModule = Unitils.getInstance().getModulesRepository().getModuleOfType(DbUnitModule.class);
        String schemaName = dbUnitModule.getDefaultSchemaName();

        // partial database export
        QueryDataSet partialDataSet = new QueryDataSet(connection, schemaName);
        try {

            int length = descriptors.length;

            for (int i = 0; i < length; i++) {
                TableDescriptor descriptor = descriptors[i];
                partialDataSet.addQry(descriptor);
            }

        } catch (AmbiguousTableNameException e) {
            logger.error("can not export the dataset", e);
            return;
        }

        try {
            FlatXmlDataSet.write(partialDataSet, new FileOutputStream(dataSetFileName));
        } catch (DataSetException e) {
            logger.error("can not export the dataset", e);
        } catch (FileNotFoundException e) {
            logger.error("can not export the dataset", e);
        } catch (IOException e) {
            logger.error("can not export the dataset", e);
        }
    }
}
