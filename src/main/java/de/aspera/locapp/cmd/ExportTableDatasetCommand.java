package de.aspera.locapp.cmd;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.SystemUtils;
import org.dbunit.DatabaseUnitException;

import de.aspera.locapp.util.ExporterController;
import de.aspera.locapp.util.json.ExportJsonCommand;
import de.aspera.locapp.util.json.ExportJsonCommandRepo;
import de.aspera.locapp.util.json.ImportJsonCommandException;
import de.aspera.locapp.util.json.JsonConnectionReadException;
import de.aspera.locapp.util.json.JsonConnectionRepo;
import de.aspera.locapp.util.json.JsonDatabase;

public class ExportTableDatasetCommand implements CommandRunnable {
	private static final Logger LOGGER = Logger.getLogger(ConfigInitCommand.class.getName());
	JsonDatabase dataConnection;
	ExportJsonCommand exportCommand;
	JsonConnectionRepo connectionRepo;
	ExportJsonCommandRepo commandRepo;
	CommandContext cmdContext;
	ByteArrayOutputStream exportStream;
	public ExportTableDatasetCommand newInstance() {
		return new ExportTableDatasetCommand();
	}

	@Override
	public void run() {

		try {
			init();
		} catch (JsonConnectionReadException | FileNotFoundException | ImportJsonCommandException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}

	}

	private void init() throws JsonConnectionReadException, FileNotFoundException, ImportJsonCommandException {
		ExporterController.readJsonDatabaseFile();
		connectionRepo = JsonConnectionRepo.getInstance();
		cmdContext = CommandContext.getInstance();
		commandRepo = ExportJsonCommandRepo.getInstance();
		commandRepo.importJsonCommands();
		String commandId = cmdContext.nextArgument();
		exportCommand = commandRepo.getCommand(commandId);
		dataConnection = connectionRepo.getJsonDatabases(exportCommand.getConnId());
		try {
			exportStream = ExporterController.startExportForTable(dataConnection, exportCommand);
			File file;
			if(SystemUtils.IS_OS_WINDOWS) {
				file = new File(exportCommand.getExportedFilePath().concat("\\DataSet-Table-"+exportCommand.getTableName()+".xml"));
			}else  {
				file = new File(exportCommand.getExportedFilePath().concat("/DataSet-Table-"+exportCommand.getTableName()+".xml"));
			}
			exportStream.writeTo(new FileOutputStream(file));
		} catch (DatabaseUnitException | SQLException | IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}

	}

}
