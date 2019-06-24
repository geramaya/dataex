package de.aspera.dataexport.cmd;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.SystemUtils;
import org.dbunit.DatabaseUnitException;

import de.aspera.dataexport.util.ExporterController;
import de.aspera.dataexport.util.json.ExportJsonCommand;
import de.aspera.dataexport.util.json.ExportJsonCommandHolder;
import de.aspera.dataexport.util.json.ImportJsonCommandException;
import de.aspera.dataexport.util.json.JsonConnectionHolder;
import de.aspera.dataexport.util.json.JsonConnectionReadException;
import de.aspera.dataexport.util.json.JsonDatabase;

public class ExportTableDatasetCommand implements CommandRunnable {
	private static final Logger LOGGER = Logger.getLogger(ConfigInitCommand.class.getName());
	private JsonDatabase dataConnection;
	private ExportJsonCommand exportCommand;
	private JsonConnectionHolder connectionRepo;
	private ExportJsonCommandHolder commandRepo;
	private CommandContext cmdContext;
	private ByteArrayOutputStream exportStream;
	public ExportTableDatasetCommand newInstance() {
		return new ExportTableDatasetCommand();
	}

	@Override
	public void run() {

		try {
			init();
		} catch (JsonConnectionReadException | ImportJsonCommandException | IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}

	}

	private void init() throws JsonConnectionReadException, ImportJsonCommandException, IOException {
		ExporterController.readJsonDatabaseFile();
		connectionRepo = JsonConnectionHolder.getInstance();
		cmdContext = CommandContext.getInstance();
		commandRepo = ExportJsonCommandHolder.getInstance();
		commandRepo.importJsonCommands();
		String commandId = cmdContext.nextArgument();
		
		if (StringUtils.isEmpty(commandId)) {
			LOGGER.log(Level.WARNING, "A commandId is required to proceed!");
			return;
		}
		
		exportCommand = commandRepo.getCommand(commandId);
		if (exportCommand == null) {
			LOGGER.log(Level.WARNING, "Your commandId:\"{0}\" could not found!", commandId);
			return;
		}
		
		dataConnection = connectionRepo.getJsonDatabases(exportCommand.getConnId());
		FileOutputStream fileOut = null;
		try {
			File file;
			exportStream = ExporterController.startExportForTable(dataConnection, exportCommand);
			if(SystemUtils.IS_OS_WINDOWS) {
				file = new File(exportCommand.getExportedFilePath().concat("\\DataSet-Table-"+exportCommand.getCommandId()+"-"+dataConnection.getIdent()+".xml"));
			}else  {
				file = new File(exportCommand.getExportedFilePath().concat("/DataSet-Table-"+exportCommand.getCommandId()+"-"+dataConnection.getIdent()+".xml"));
			}
			fileOut = new FileOutputStream(file);
			exportStream.writeTo(fileOut);
		} catch (DatabaseUnitException | SQLException | IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		} finally {
			if (exportStream != null)
				IOUtils.closeQuietly(exportStream);
			if (fileOut != null)
				IOUtils.closeQuietly(fileOut);
		}
	}

}
