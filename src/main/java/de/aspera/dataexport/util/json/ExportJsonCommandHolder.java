package de.aspera.dataexport.util.json;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.lang.reflect.Type;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.SystemUtils;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;

import de.aspera.dataexport.util.Resources;

public final class ExportJsonCommandHolder {
	private Map<String, ExportJsonCommand> cmdRepo = new HashMap<>();
	private static ExportJsonCommandHolder instance;
	private Gson gson = new Gson();

	private ExportJsonCommandHolder() {

	}

	public static ExportJsonCommandHolder getInstance() {
		if (instance == null) {
			instance = new ExportJsonCommandHolder();
		}
		return instance;
	}

	public void importJsonCommands() throws FileNotFoundException, ImportJsonCommandException {
		cmdRepo = new HashMap<>();
		JsonReader reader = new JsonReader(new FileReader(getJasonFileConn()));
		reader.setLenient(true);
		Type listType = new TypeToken<List<ExportJsonCommand>>() {
		}.getType();
		List<ExportJsonCommand> commands = gson.fromJson(reader, listType);
		addCommandList(commands);
	}

	public void addCommandList(List<ExportJsonCommand> commands) throws ImportJsonCommandException {
		for (ExportJsonCommand cmd : commands) {
			String commId = cmd.getCommandId();
			if (cmdRepo.keySet().contains(commId)) {
				throw new ImportJsonCommandException("duplicated Command Id");
			} else if (commId.isEmpty()) {
				throw new ImportJsonCommandException("Id for Command not defined");
			} else if (cmd.getExportedFilePath().isEmpty() || !new File(cmd.getExportedFilePath()).isDirectory()) {
				throw new ImportJsonCommandException("Path for the exported File is empty or does not exist");
			} else if (cmd.getConnId().isEmpty()) {
				throw new ImportJsonCommandException("Connection Id is empty");
			} else {
				for(TableQuery table : cmd.getTables()) {
					if(table.getTableName().isEmpty())
						throw new ImportJsonCommandException("Table name is empty");
				}
				cmdRepo.put(commId, cmd);
			}
		}
	}

	public ExportJsonCommand getCommand(String id) {
		return cmdRepo.get(id);
	}

	public File getJasonFileConn() {
		String filePath;
		if (SystemUtils.IS_OS_WINDOWS) {
			filePath = System.getProperty("user.home") + "\\." + Resources.PROJECT_NAME
					+ "\\dataExporter_ExportCommands.json";
		} else {
			filePath = System.getProperty("user.home") + "/." + Resources.PROJECT_NAME
					+ "dataExporter_ExportCommands.json";
		}
		Path pathOfFile = Paths.get(filePath);
		return pathOfFile.toFile();
	}

}
