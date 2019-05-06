package de.aspera.locapp.util;

import java.sql.SQLException;
import java.util.List;

import org.dbunit.DatabaseUnitException;

public class ExporterController {
	private DataSetExporter exporter = new DataSetExporter();
	DataConnection dataConn;
	
	public void makeConnection(DataConnection dataconn) {
		this.dataConn=dataconn;
		exporter.setConnection(JDBCConnection.getConnection(dataconn.getDatabaseUrl(), dataconn.getUsername(), dataconn.getPassword()));
	}
	public void startExport(String filePath, String tableName, String orderByClause,String whereClause ) throws DatabaseUnitException, SQLException {
		String schemaName = dataConn.getDatabaseUrl().substring(dataConn.getDatabaseUrl().lastIndexOf("/")+1);
		TableDescriptor discriptor = new TableDescriptor(tableName);
		discriptor.setOrderByClause(orderByClause);
		discriptor.setWhereClause(whereClause);
		discriptor.setSchemaName(schemaName);
		exporter.exportDataSet(filePath, "DataSet-"+ tableName, discriptor);
	}

}
