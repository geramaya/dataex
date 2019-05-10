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
 * @author Petr Stastny
 * 
 */
public class DataSetExporter {

	private static final Logger logger = LoggerFactory.getLogger(DataSetExporter.class);
	private Connection connection;

	public DataSetExporter(DataConnection databaseConnection) {
		this.connection = getConnection(databaseConnection);
	}

	/**
	 * Export a unitils/dbunit datasheet as xml file
	 * 
	 * @param descriptors
	 * @return
	 * @throws DatabaseUnitException
	 * @throws SQLException
	 */
	public ByteArrayOutputStream exportDataSet(TableDescriptor... descriptors)
			throws DatabaseUnitException, SQLException {
		IDatabaseConnection conn = new MySqlConnection(connection, descriptors[0].getSchemaName());
		ByteArrayOutputStream outputStream = null;
		// partial database export
		QueryDataSet partialDataSet = new QueryDataSet(conn, descriptors[0].getSchemaName());
		try {
			for (TableDescriptor descriptor : descriptors) {
				partialDataSet.addQry(descriptor);
			}

		} catch (AmbiguousTableNameException e) {
			logger.error("can not export the dataset", e);
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
	 * Create and returns a standard jdbc database connection
	 * @param databaseConnection
	 * @return
	 */
	private Connection getConnection(DataConnection databaseConnection) {
		return JDBCConnection.getConnection(databaseConnection.getDatabaseUrl(), databaseConnection.getUsername(),
				databaseConnection.getPassword());
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
		this.connection = connection;
	}
}
