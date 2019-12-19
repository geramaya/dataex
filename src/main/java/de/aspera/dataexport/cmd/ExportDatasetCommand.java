package de.aspera.dataexport.cmd;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;

import de.aspera.dataexport.util.ExporterController;
import de.aspera.dataexport.util.json.ExportJsonCommand;
import de.aspera.dataexport.util.json.ExportJsonCommandHolder;
import de.aspera.dataexport.util.json.JsonConnectionHolder;
import de.aspera.dataexport.util.json.JsonDatabase;

public class ExportDatasetCommand implements CommandRunnable {
	private static final Logger LOGGER = Logger.getLogger(ExportDatasetCommand.class.getName());
	private JsonDatabase dataConnection;
	private ExportJsonCommand exportCommand;
	private JsonConnectionHolder connectionRepo;
	private ExportJsonCommandHolder commandRepo;
	private CommandContext cmdContext;
	private ByteArrayOutputStream exportStream;

	@Override
	public void run() throws CommandException {
		init();
	}

	private void init() throws CommandException {
		try {

			ExporterController.readJsonDatabaseFile();
			ExporterController.readJsonCommandsFile();
			connectionRepo = JsonConnectionHolder.getInstance();
			cmdContext = CommandContext.getInstance();
			commandRepo = ExportJsonCommandHolder.getInstance();
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
			File file;
			exportStream = ExporterController.startExportForTable(dataConnection, exportCommand);
			file = new File(exportCommand.getExportedFilePath().concat(
						File.separator +"DataSet-Table-" + exportCommand.getCommandId() + "-" + dataConnection.getIdent() + ".xml"));
			fileOut = new FileOutputStream(file);
			exportStream.writeTo(fileOut);
			if (exportStream != null)
				IOUtils.closeQuietly(exportStream);
			if (fileOut != null)
				IOUtils.closeQuietly(fileOut);
		} catch (Exception e) {
			throw new CommandException(e.getMessage(), e);
		}
	}

}
