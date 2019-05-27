package de.aspera.dataexport.util;

import java.io.ByteArrayOutputStream;
import java.sql.SQLException;

import org.dbunit.DatabaseUnitException;

import de.aspera.dataexport.util.json.ExportJsonCommand;
import de.aspera.dataexport.util.json.JsonConnectionHolder;
import de.aspera.dataexport.util.json.JsonConnectionReadException;
import de.aspera.dataexport.util.json.JsonDatabase;

public class ExporterController {

	/**
	 * Get a buffered output stream to transform data or persist on the filesystem.
	 * 
	 * @param tableName
	 * @param columnsComaSeperated
	 *            e.g. "firstname, lastname, ...." or "*" for all columns
	 * @param whereClause
	 * @param orderByClause
	 * @return
	 * @throws DatabaseUnitException
	 * @throws SQLException
	 */
	public static ByteArrayOutputStream startExportForTable(JsonDatabase databaseConnection,
			ExportJsonCommand exportCommand) throws DatabaseUnitException, SQLException {

		if (databaseConnection == null)
			throw new IllegalArgumentException("The databaseConnection can not be null");

		DataSetExporter exporter = new DataSetExporter(databaseConnection);
		TableDescriptor discriptor = new TableDescriptor(exportCommand.getTableName());
		discriptor.setOrderByClause(exportCommand.getOrderByClause());
		discriptor.setWhereClause(exportCommand.getWhereClause());
		discriptor.setSchemaName(databaseConnection.getSchemaName());
		discriptor.addField(exportCommand.getColumns());
		return exporter.exportDataSet(discriptor);
	}

	public static void readJsonDatabaseFile() throws JsonConnectionReadException {
		JsonConnectionHolder.getInstance().initJsonDatabases();
	}

}
