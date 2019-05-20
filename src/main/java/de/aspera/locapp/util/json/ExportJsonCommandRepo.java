package de.aspera.locapp.util.json;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.SystemUtils;

import com.google.gson.Gson;
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
	public void importJsonComannds() throws FileNotFoundException, ImportJsonCommandException {
		JsonReader reader = new JsonReader(new FileReader(getJasonFileConn()));
		reader.setLenient(true);
		ExportJsonCommand[] commands = gson.fromJson(reader, ExportJsonCommand.class);
		for(int i=0; i<commands.length;i++) {
			String commId = commands[i].getCommandId();
			if(cmdRepo.keySet().contains(commId)) {
				throw new ImportJsonCommandException("duplicated Command Id");
			}else if(commId.isEmpty()){
				throw new ImportJsonCommandException("Id for Command not defined");
			}else{
				cmdRepo.put(commId, commands[i]);
			}
		}
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
