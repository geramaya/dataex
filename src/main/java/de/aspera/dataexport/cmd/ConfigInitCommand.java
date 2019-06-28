package de.aspera.dataexport.cmd;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.SystemUtils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import de.aspera.dataexport.dao.ConfigFacade;
import de.aspera.dataexport.dao.DatabaseException;
import de.aspera.dataexport.dto.Config;
import de.aspera.dataexport.util.Resources;
import de.aspera.dataexport.util.json.ExportJsonCommand;
import de.aspera.dataexport.util.json.JsonDatabase;
import de.aspera.dataexport.util.json.TableQuery;

public class ConfigInitCommand implements CommandRunnable {

	private static final Logger LOGGER = Logger.getLogger(ConfigInitCommand.class.getName());
	public final static String PROJECT_NAME = "dataExporter";
	public static final String EXCLUDED_KEY = "Excluded_Paths";
	public static final String[] EXCLUDED_VALUES = { "target" };

	@Override
	public void run() {
		this.init();

	}

	private void init() {
		try {
			List<Config> configs = new ArrayList<>();
			ConfigFacade configFacade = new ConfigFacade();
			Config config = new Config();

			config.setKey(EXCLUDED_KEY);
			config.setValue(EXCLUDED_VALUES);
			configs.add(config);

			// if no entry found, so add the default entries
			if (configFacade.findAll().isEmpty()) {
				configFacade.saveConfig(configs);
			}
			// write initial Command and Connection files for export Command
			writeJsonExportFiles();
		} catch (DatabaseException | IOException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
	}

	private void writeJsonExportFiles() throws IOException {
		String filePath;
		FileWriter fileWriter;
		File file;
		Gson gson = new GsonBuilder().disableHtmlEscaping().setPrettyPrinting().create();
		List<TableQuery> tablesList = new ArrayList<TableQuery>();

		// Write Connection file
		if (SystemUtils.IS_OS_WINDOWS) {
			filePath = System.getProperty("user.home") + "\\." + PROJECT_NAME + "\\dataExporter_Connections.json";
		} else {
			filePath = System.getProperty("user.home") + "/." + PROJECT_NAME + "dataExporter_Connections.json";
		}
		file = new File(filePath);
		if(!file.exists()) {
			fileWriter = new FileWriter(file);
			JsonDatabase jsonDB = new JsonDatabase();
			jsonDB.setIdent("uniqe-connection-Id");
			jsonDB.setDbDriver("Db-Driver");
			jsonDB.setDbPassword("root");
			jsonDB.setDbSchema("schema-1");
			jsonDB.setDbUrl("database-URL");
			jsonDB.setDbUser("root");
			gson.toJson(jsonDB, fileWriter);
			fileWriter.close();
		}
		// Write Command file
		if (SystemUtils.IS_OS_WINDOWS) {
			filePath = System.getProperty("user.home") + "\\." + Resources.PROJECT_NAME
					+ "\\dataExporter_ExportCommands.json";
		} else {
			filePath = System.getProperty("user.home") + "/." + Resources.PROJECT_NAME
					+ "dataExporter_ExportCommands.json";
		}
		file = new File(filePath);
		if(!file.exists()) {
			fileWriter = new FileWriter(file);
			ExportJsonCommand command = new ExportJsonCommand();
			command.setConnId("uniqe-connection-Id");
			for (int i = 0; i < 2; i++) {
				TableQuery tab = new TableQuery();
				tab.setTableName("tab-" + i);
				tab.setColumns("col1,col2,col3");
				tab.setOrderByClaus("col1 asc");
				tab.setWhereClaus("col1='wert1' and col2='wert2'");
				tablesList.add(tab);
			}
			command.setTables(tablesList);
			command.setCommandId("uniqe-command-Id");
			command.setExportedFilePath("Path where the Dataset will be exported");
			gson.toJson(command, fileWriter);
			fileWriter.close();

		}
	}
}
