package de.aspera.locapp.cmd;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.dbunit.DatabaseUnitException;

import de.aspera.locapp.util.ExporterController;
import de.aspera.locapp.util.json.JsonConnectionReadException;
import de.aspera.locapp.util.json.JsonConnectionRepo;
import de.aspera.locapp.util.json.JsonDatabase;

public class ExportTableDatasetCommand implements CommandRunnable {
	private static final Logger LOGGER = Logger.getLogger(ConfigInitCommand.class.getName());
	JsonDatabase dataConnection;
	JsonConnectionRepo connectionRepo;
	CommandContext cmdContext;
	ByteArrayOutputStream exportStream;
	public ExportTableDatasetCommand newInstance() {
		return new ExportTableDatasetCommand();
	}

	@Override
	public void run() {

		try {
			init();
		} catch (JsonConnectionReadException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}

	}

	private void init() throws JsonConnectionReadException {
		ExporterController.readJsonDatabaseFile();
		connectionRepo = JsonConnectionRepo.getInstance();
		cmdContext = CommandContext.getInstance();
		String connectionId = cmdContext.nextArgument();
		String tableName = cmdContext.nextArgument();
		String columnsComaSeperated="*";
		String whereClause=null;
		String orderByClause=null;
		String filePath=null;
		for(int i =0 ; i<cmdContext.sizeOfArguments()-2; i++) {
			String currentArg = cmdContext.nextArgument();
			if(currentArg.contains(",")) {
				columnsComaSeperated=currentArg;
			}else if(currentArg.contains("=")||currentArg.contains(">")||currentArg.contains("<") ){
				whereClause=currentArg;
			}else if (currentArg.toLowerCase().contains("asc")||currentArg.toLowerCase().contains("desc")) {
				orderByClause=currentArg;
			}else if (currentArg.contains("/")) {
				filePath = currentArg;
			}
		}
		dataConnection = connectionRepo.getJsonDatabases(connectionId);
		try {
			exportStream = ExporterController.startExportForTable(dataConnection, tableName, columnsComaSeperated,
					whereClause, orderByClause);
			File file = new File(filePath);
			exportStream.writeTo(new FileOutputStream(file));
		} catch (DatabaseUnitException | SQLException | IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}

	}

}
