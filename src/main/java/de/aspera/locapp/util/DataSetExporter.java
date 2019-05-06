package de.aspera.locapp.util;

import java.io.ByteArrayOutputStream;
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







/**
 * Use this to export data from database to a flat XML data set..
 * 
 * @author Petr Stastny
 * 
 */
public class DataSetExporter {

    private static final Logger logger   = LoggerFactory.getLogger(DataSetExporter.class);
    private Connection connection ;
	private ByteArrayOutputStream outputStream;
    public final static DataSetExporter   INSTANCE = new DataSetExporter();

    public ByteArrayOutputStream exportDataSet(TableDescriptor... descriptors) throws DatabaseUnitException, SQLException {
    	  IDatabaseConnection conn = new MySqlConnection(connection, descriptors[0].getSchemaName());
          // partial database export
          QueryDataSet partialDataSet = new QueryDataSet(conn, descriptors[0].getSchemaName());
          try {

              int length = descriptors.length;

              for (int i = 0; i < length; i++) {
                  TableDescriptor descriptor = descriptors[i];
                  partialDataSet.addQry(descriptor);
              }

          } catch (AmbiguousTableNameException e) {
              logger.error("can not export the dataset", e);
              return null;
          }

          try {
          	outputStream = new ByteArrayOutputStream();
              FlatXmlDataSet.write(partialDataSet, outputStream);
          } catch (DataSetException e) {
              logger.error("can not export the dataset", e);
          } catch (IOException e) {
              logger.error("can not export the dataset", e);
          }
          return outputStream;
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
}
