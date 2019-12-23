package de.aspera.dataexport.cmd;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.DefaultMetadataHandler;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.dbunit.ext.mysql.MySqlMetadataHandler;
import org.dbunit.operation.DatabaseOperation;

import de.aspera.dataexport.util.JDBCConnection;
import de.aspera.dataexport.util.json.JsonConnectionHolder;
import de.aspera.dataexport.util.json.JsonDatabase;

public class ImportDatasetCommand implements CommandRunnable {
	private static final Logger LOGGER = Logger.getLogger(ImportDatasetCommand.class.getName());
	private CommandContext cmdContext;
	private JsonConnectionHolder connectionRepo;
	private JsonDatabase dataConnection;
	private boolean cleanInsert = true;


	@Override
	public void run() throws CommandException {
		try {

			final String filePath;
			cmdContext = CommandContext.getInstance();
			JsonConnectionHolder.getInstance();
			connectionRepo = JsonConnectionHolder.getInstance();
			connectionRepo.initJsonDatabases();
			String cleanOption = cmdContext.nextArgument();
			if (cleanOption.toLowerCase().equals("-c") || cleanOption.toLowerCase().equals("-clean")) {
				filePath = cmdContext.nextArgument();
			} else {
				cleanInsert = false;
				filePath = cleanOption;
			}
			dataConnection = connectionRepo.getJsonDatabases(cmdContext.nextArgument());
			IDataSet dataSet = new FlatXmlDataSetBuilder().setColumnSensing(true).build(new FileInputStream(filePath));
			IDatabaseConnection connection = getConnection(dataConnection);
			if (cleanInsert) {
				DatabaseOperation.CLEAN_INSERT.execute(connection, dataSet);
			} else {
				DatabaseOperation.INSERT.execute(connection, dataSet);
			}
		} catch (Exception e) {
			throw new CommandException(e.getMessage(), e);
		}

	}

	/**
	 * Create and returns a standard jdbc database connection
	 * 
	 * @param databaseConnection
	 * @return
	 * @throws DatabaseUnitException
	 */
	private IDatabaseConnection getConnection(JsonDatabase databaseConnection)
			throws DatabaseUnitException, SQLException {
		Connection conn = JDBCConnection.getConnection(databaseConnection.getDbUrl(), databaseConnection.getDbUser(),
				databaseConnection.getDbPassword());
		DatabaseConnection connection = new DatabaseConnection(conn, databaseConnection.getDbSchema());
		DatabaseConfig config = connection.getConfig();
		if(dataConnection.getDbUrl().contains("mysql")) {
			config.setProperty(DatabaseConfig.PROPERTY_METADATA_HANDLER, new MySqlMetadataHandler());
		}else {
			config.setProperty(DatabaseConfig.PROPERTY_METADATA_HANDLER, new DefaultMetadataHandler());
		}
		return connection;

	}

}
