package de.aspera.locapp.util;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.dbunit.DatabaseUnitException;
import org.dbunit.database.AmbiguousTableNameException;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.xml.FlatXmlDataSet;
import org.dbunit.ext.mysql.MySqlConnection;
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
    private Connection connection ;
	private String schemaName;
    public final static DataSetExporter   INSTANCE = new DataSetExporter();

    public void exportDataSet(String path, String fileName, TableDescriptor... descriptors) throws DatabaseUnitException, SQLException {
        exportDataSetInternal(this.generateDataSetFileName(path, fileName), descriptors);
    }

    public void exportExpectedDataSet(String path, Class<?> callerClass, String testMethod,
            TableDescriptor... descriptors) throws DatabaseUnitException, SQLException {
        exportDataSetInternal(this.generateExpectedDataSetFileName(path, callerClass, testMethod), descriptors);
    }

    protected String generateDataSetFileName(String path, String fileName) {
        StringBuilder builder = new StringBuilder();

        builder.append(path).append("/").append(fileName+".xml.sample");

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

    

    /** 
     * Reuse the connection where possible.
     * 
     */
    public Connection getConnection() {
    	
        return connection;
    }
    /**
     * 
     * @param Url
     * @param username
     * @param password
     * @return
     */
	public void setConnection(Connection connection) {
		this.connection= connection;
	}
	
	public void setSchemaName(String schemaName) {
		this.schemaName=schemaName;
	}

    protected void exportDataSetInternal(String dataSetFileName, TableDescriptor... descriptors) throws DatabaseUnitException, SQLException {    	
        IDatabaseConnection conn = new MySqlConnection(connection, schemaName);
        // partial database export
        QueryDataSet partialDataSet = new QueryDataSet(conn, schemaName);
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
