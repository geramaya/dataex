package de.aspera.dataexport.util;

import java.io.ByteArrayOutputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.dbunit.DatabaseUnitException;

import de.aspera.dataexport.util.json.ExportJsonCommand;
import de.aspera.dataexport.util.json.JsonConnectionHolder;
import de.aspera.dataexport.util.json.JsonConnectionReadException;
import de.aspera.dataexport.util.json.JsonDatabase;

public class ExporterController {

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
		Iterator<String> whereClauseIter = null;
		Iterator<String> oderByClauseIter = null;
		Iterator<String> tableColumsIter = null;
		if (databaseConnection == null)
			throw new IllegalArgumentException("The databaseConnection can not be null");
		DataSetExporter exporter = new DataSetExporter(databaseConnection);

		if (exportCommand.getOrderByClauses() != null)
			oderByClauseIter = exportCommand.getOrderByClauses().iterator();
		if (exportCommand.getWhereClauses() != null)
			whereClauseIter = exportCommand.getWhereClauses().iterator();
		if (exportCommand.getColumns() != null)
			tableColumsIter = exportCommand.getColumns().iterator();
		for (String tabelName : exportCommand.getTableNames()) {
			TableDescriptor descriptor = new TableDescriptor(tabelName);
			descriptor.setSchemaName(databaseConnection.getDbSchema());
			if (oderByClauseIter != null)
				if (oderByClauseIter.hasNext())
					descriptor.setOrderByClause(oderByClauseIter.next());
			if (whereClauseIter != null)
				if (whereClauseIter.hasNext())
					descriptor.setWhereClause(whereClauseIter.next());
			if (tableColumsIter != null)
				if (tableColumsIter.hasNext())
					descriptor.addField(tableColumsIter.next());
				else
					descriptor.addField("*");
			descriptors.add(descriptor);
		}
		return exporter.exportDataSet(descriptors);
	}

	public static void readJsonDatabaseFile() throws JsonConnectionReadException {
		JsonConnectionHolder.getInstance().initJsonDatabases();
	}

}
