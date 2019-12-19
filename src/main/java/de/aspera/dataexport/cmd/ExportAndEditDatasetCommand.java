package de.aspera.dataexport.cmd;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.dbunit.DatabaseUnitException;
import org.dbunit.dataset.xml.FlatXmlDataSet;

import de.aspera.dataexport.groovy.GroovyReader;
import de.aspera.dataexport.util.ExporterController;
import de.aspera.dataexport.util.dataset.editor.DatasetEditorFacade;
import de.aspera.dataexport.util.json.ExportJsonCommand;
import de.aspera.dataexport.util.json.ExportJsonCommandHolder;
import de.aspera.dataexport.util.json.ImportJsonCommandException;
import de.aspera.dataexport.util.json.JsonConnectionHolder;
import de.aspera.dataexport.util.json.JsonConnectionReadException;
import de.aspera.dataexport.util.json.JsonDatabase;

public class ExportAndEditDatasetCommand implements CommandRunnable {
	private static final Logger LOGGER = Logger.getLogger(ExportAndEditDatasetCommand.class.getName());
	private JsonDatabase dataConnection;
	private ExportJsonCommand exportCommand;
	private JsonConnectionHolder connectionRepo;
	private ExportJsonCommandHolder commandRepo;
	private CommandContext cmdContext;
	private DatasetEditorFacade editorFacade;
	private ByteArrayOutputStream exportStream;
	private GroovyReader groovyReader;
	private File file;
	private FileOutputStream fileOut;
	private ByteArrayInputStream inputStream;

	@Override
	public void run() throws CommandException {
		init();
		startExportFromDataBase();
		startEditingDataset();

	}

	private void init() throws CommandException {
		try {
			ExporterController.readJsonDatabaseFile();
			ExporterController.readJsonCommandsFile();
			connectionRepo = JsonConnectionHolder.getInstance();
			cmdContext = CommandContext.getInstance();
			commandRepo = ExportJsonCommandHolder.getInstance();
			cmdContext = CommandContext.getInstance();
			commandRepo = ExportJsonCommandHolder.getInstance();
			editorFacade = new DatasetEditorFacade();
		} catch (JsonConnectionReadException | FileNotFoundException | ImportJsonCommandException e) {
			throw new CommandException(e.getMessage(), e);
		}
	}

	private void startExportFromDataBase() throws CommandException {
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
		try {
			exportStream = ExporterController.startExportForTable(dataConnection, exportCommand);
		} catch (DatabaseUnitException | SQLException e) {
			throw new CommandException(e.getMessage(), e);
		} finally {
			if (exportStream != null)
				IOUtils.closeQuietly(exportStream);
			if (fileOut != null)
				IOUtils.closeQuietly(fileOut);
		}
	}

	private void startEditingDataset() throws CommandException {
		try {
			// convert output to input stream without writing to the desk
			inputStream = new ByteArrayInputStream(exportStream.toByteArray());
			editorFacade.readDataset(inputStream);
			groovyReader = new GroovyReader();
			editorFacade.setConnectionOfDB(ExporterController.getConnection());
			groovyReader.executeGroovyScript(editorFacade);
			// Write results Back to file
			file = new File(exportCommand.getExportedFilePath().concat(File.separator + "DataSet-Table-"
					+ exportCommand.getCommandId() + "-" + dataConnection.getIdent() + ".xml"));
			fileOut = new FileOutputStream(file);
			FlatXmlDataSet.write(editorFacade.getDataSet(), fileOut);
		} catch (Exception e) {
			throw new CommandException(e.getMessage(), e);
		} finally {
			if (inputStream != null)
				IOUtils.closeQuietly(inputStream);
			if (fileOut != null)
				IOUtils.closeQuietly(fileOut);
		}

	}
}
