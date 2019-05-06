package de.aspera.locapp.util;

import java.io.ByteArrayOutputStream;
import java.sql.SQLException;

import org.dbunit.DatabaseUnitException;

public class ExporterController {
	private DataSetExporter exporter = new DataSetExporter();
	private DataConnection dataConn;
	
	public void makeConnection(DataConnection dataconn) {
		this.dataConn=dataconn;
		exporter.setConnection(JDBCConnection.getConnection(dataconn.getDatabaseUrl(), dataconn.getUsername(), dataconn.getPassword()));
	}
	public ByteArrayOutputStream startExport(String tableName, String whereClause,String orderByClause ) throws DatabaseUnitException, SQLException {
		String schemaName = dataConn.getDatabaseUrl().substring(dataConn.getDatabaseUrl().lastIndexOf("/")+1);
		TableDescriptor discriptor = new TableDescriptor(tableName);
		discriptor.setOrderByClause(orderByClause);
		discriptor.setWhereClause(whereClause);
		discriptor.setSchemaName(schemaName);
		discriptor.addField("*");
		return exporter.exportDataSet( discriptor);
	}

}
