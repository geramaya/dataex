package de.aspera.locapp.util;

import java.io.ByteArrayOutputStream;
import java.sql.SQLException;

import org.dbunit.DatabaseUnitException;

import de.aspera.locapp.util.json.JsonConnectionReadException;
import de.aspera.locapp.util.json.JsonConnectionRepo;
import de.aspera.locapp.util.json.JsonDatabase;

public class ExporterController {

	/**
	 * Get a buffered output stream to transform data or persist on the filesystem.
	 * 
	 * @param tableName
	 * @param columnsComaSeperated e.g. "firstname, lastname, ...." or "*" for all columns
	 * @param whereClause
	 * @param orderByClause
	 * @return
	 * @throws DatabaseUnitException
	 * @throws SQLException
	 */
	public static ByteArrayOutputStream startExportForTable(JsonDatabase databaseConnection, String tableName,
			String columnsComaSeperated, String whereClause, String orderByClause)
			throws DatabaseUnitException, SQLException {
		
		if (databaseConnection == null)
			throw new IllegalArgumentException("The databaseConnection can not be null");
		
		if (tableName == null || columnsComaSeperated == null) 
			throw new IllegalArgumentException("The tableName and columns definition are required for an export!");
		
		
		DataSetExporter exporter = new DataSetExporter(databaseConnection);
		String schemaName = databaseConnection.getDbUrl()
				.substring(databaseConnection.getDbUrl().lastIndexOf("/") + 1);
		TableDescriptor discriptor = new TableDescriptor(tableName);
		discriptor.setOrderByClause(orderByClause);
		discriptor.setWhereClause(whereClause);
		discriptor.setSchemaName(schemaName);
		discriptor.addField(columnsComaSeperated);
		return exporter.exportDataSet(discriptor);
	}
	public static void readJsonDatabaseFile() throws JsonConnectionReadException {
		JsonConnectionRepo.getInstance().initJsonDatabases();
	}


}
