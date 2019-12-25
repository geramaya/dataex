package de.aspera.dataexport.cmd;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
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
import de.aspera.dataexport.util.dataset.editor.TableConstrainsDescription;
import de.aspera.dataexport.util.dataset.editor.TableKeysInvestigator;
import de.aspera.dataexport.util.dataset.editor.TableKeysInvestigatorException;
import de.aspera.dataexport.util.json.JsonConnectionHolder;
import de.aspera.dataexport.util.json.JsonDatabase;

public class ImportDatasetCommand implements CommandRunnable {
	private static final Logger LOGGER = Logger.getLogger(ImportDatasetCommand.class.getName());
	private static final Scanner scanner = new Scanner(System.in);
	private CommandContext cmdContext;
	private JsonConnectionHolder connectionRepo;
	private JsonDatabase dataConnection;
	private boolean cleanInsert = true;
	private boolean resumedWithCleanInsert = false;
	private TableKeysInvestigator tableInvestigator;
	private Map<String, TableConstrainsDescription> tableConstriants;

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
			List<String> tabNames = Arrays.asList(dataSet.getTableNames());
			tableConstriants = tableInvestigator.createTableConstrainsDescriptions(tabNames);
			if (cleanInsert) {
				checkWarnings();
				if (resumedWithCleanInsert) {
					tableInvestigator.disableFKeyConstriantCheck();
					DatabaseOperation.CLEAN_INSERT.execute(connection, dataSet);
					tableInvestigator.enableFKeyConstriantCheck();
				}
			} else {
				DatabaseOperation.REFRESH.execute(connection, dataSet);
			}
		} catch (Exception e) {
			throw new CommandException(e.getMessage(), e);
		}

	}

	private void checkWarnings() {
		for (String table : tableConstriants.keySet()) {
			Set<String> refrencedFrom = tableConstriants.get(table).getReferencedFromTables();
			if (!refrencedFrom.isEmpty()) {
				String msg = table + " is refrenced from the following tables:";
				Iterator<String> iter = refrencedFrom.iterator();
				while (iter.hasNext()) {
					msg += "\n" + iter.next();
				}
				LOGGER.log(Level.INFO, msg);
			}
		}
		System.out.print("\n>> Are you sure you want to continue (Y/N): ");
		String cmdline = scanner.nextLine().trim();
		if (cmdline.equalsIgnoreCase("y")) {
			resumedWithCleanInsert = true;
		}
	}

	/**
	 * Create and returns a standard jdbc database connection
	 * 
	 * @param databaseConnection
	 * @return
	 * @throws DatabaseUnitException
	 * @throws TableKeysInvestigatorException
	 */
	private IDatabaseConnection getConnection(JsonDatabase databaseConnection)
			throws DatabaseUnitException, SQLException, TableKeysInvestigatorException {
		tableInvestigator = new TableKeysInvestigator();
		Connection conn = JDBCConnection.getConnection(databaseConnection.getDbUrl(), databaseConnection.getDbUser(),
				databaseConnection.getDbPassword());
		tableInvestigator.setConnection(conn);
		DatabaseConnection connection = new DatabaseConnection(conn, databaseConnection.getDbSchema());
		DatabaseConfig config = connection.getConfig();
		if (dataConnection.getDbUrl().contains("mysql")) {
			config.setProperty(DatabaseConfig.PROPERTY_METADATA_HANDLER, new MySqlMetadataHandler());
		} else {
			config.setProperty(DatabaseConfig.PROPERTY_METADATA_HANDLER, new DefaultMetadataHandler());
		}
		return connection;

	}

}
