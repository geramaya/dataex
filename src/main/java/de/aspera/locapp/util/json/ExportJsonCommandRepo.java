package de.aspera.locapp.util.json;

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

public class ExportJsonCommandRepo {
	Map<String, ExportJsonCommand> cmdRepo = new HashMap<>();
	public final String PROJECT_NAME = "dataExporter";
	private static ExportJsonCommandRepo instance;
	Gson gson = new Gson();

	private ExportJsonCommandRepo() {

	}

	public static ExportJsonCommandRepo getInstance() {
		if (instance == null) {
			instance = new ExportJsonCommandRepo();
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
		for (ExportJsonCommand cmd : commands) {
			String commId = cmd.getCommandId();
			if (cmdRepo.keySet().contains(commId)) {
				throw new ImportJsonCommandException("duplicated Command Id");
			} else if (commId.isEmpty()) {
				throw new ImportJsonCommandException("Id for Command not defined");
			} else if (cmd.getExportedFilePath().isEmpty() || !new File(cmd.getExportedFilePath()).isDirectory()) {
				throw new ImportJsonCommandException("Path for the exported File is empty or does not exist");
			} else if (cmd.getTableName().isEmpty()) {
				throw new ImportJsonCommandException("Table name is empty");
			} else if (cmd.getConnId().isEmpty()) {
				throw new ImportJsonCommandException("Connection Id is empty");
			} else {
				if (cmd.getColumns().isEmpty())
					cmd.setColumns("*");
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
			filePath = System.getProperty("user.home") + "\\." + PROJECT_NAME + "\\dataExporter_ExportCommands.json";
		} else {
			filePath = System.getProperty("user.home") + "/." + PROJECT_NAME + "dataExporter_ExportCommands.json";
		}
		Path pathOfFile = Paths.get(filePath);
		return pathOfFile.toFile();
	}

}
