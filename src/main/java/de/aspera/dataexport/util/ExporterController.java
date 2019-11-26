package de.aspera.dataexport.util;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.dbunit.DatabaseUnitException;

import de.aspera.dataexport.util.json.ExportJsonCommand;
import de.aspera.dataexport.util.json.ExportJsonCommandHolder;
import de.aspera.dataexport.util.json.ImportJsonCommandException;
import de.aspera.dataexport.util.json.JsonConnectionHolder;
import de.aspera.dataexport.util.json.JsonConnectionReadException;
import de.aspera.dataexport.util.json.JsonDatabase;
import de.aspera.dataexport.util.json.TableQuery;

public class ExporterController {
	private static DataSetExporter exporter;

	/**
	 * Get a buffered output stream to transform data or persist on the filesystem.
	 * A Command must have the same Schema and connection put can have more than one
	 * Table
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
		List<TableDescriptor> descriptors = new ArrayList<>();
		if (databaseConnection == null)
			throw new IllegalArgumentException("The databaseConnection can not be null");
		exporter = new DataSetExporter(databaseConnection);
		for (TableQuery table : exportCommand.getTables()) {
			TableDescriptor descriptor = new TableDescriptor(table.getTableName());
			descriptor.setSchemaName(databaseConnection.getDbSchema());
			if (table.getOrderByCondition() != null)
				descriptor.setOrderByClause(table.getOrderByCondition());
			if (table.getWhereCondition() != null)
				descriptor.setWhereClause(table.getWhereCondition());
			if (!table.getColumns().isEmpty())
				descriptor.addField(table.getColumns());
				else
					descriptor.addField("*");
			descriptors.add(descriptor);
		}
		return exporter.exportDataSet(descriptors);
	}

	public static void readJsonDatabaseFile() throws JsonConnectionReadException {
		JsonConnectionHolder.getInstance().deleteConnections();
		JsonConnectionHolder.getInstance().initJsonDatabases();
	}

	public static void readJsonCommandsFile() throws FileNotFoundException, ImportJsonCommandException {
		ExportJsonCommandHolder.getInstance().deleteCommands();
		ExportJsonCommandHolder.getInstance().importJsonCommands();
		
	}
	
	public static Connection getConnection() {
		return exporter.getConnection();
	}
	

}
