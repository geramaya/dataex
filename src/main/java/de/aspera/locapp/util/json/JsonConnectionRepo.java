package de.aspera.locapp.util.json;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.SystemUtils;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;

public class JsonConnectionRepo {
	private static JsonConnectionRepo instance;
	public final static String PROJECT_NAME = "dataExporter";
	private Map<String, JsonDatabase> dbConnections = new HashMap<>();
	private static final Logger logger = Logger.getLogger(JsonConnectionRepo.class.getName());

	private JsonConnectionRepo() {

	}

	/**
	 * This method create and returns the singleton instance.
	 * 
	 * @return
	 * @throws NoDriverDefinedException
	 * @throws NoIdDefinedException
	 * @throws DuplicateConnIdException
	 */
	public static JsonConnectionRepo getInstance() throws JsonConnectionReadException {
		if (instance == null) {
			instance = new JsonConnectionRepo();
		}
		return instance;
	}

	public void initJsonDatabases() {
		dbConnections = new HashMap<>();
		JsonParser jsonParser = new JsonParser();
		try {
			JsonReader reader = new JsonReader(new FileReader(getJasonFileConn()));
			reader.setLenient(true);
			Object obj = jsonParser.parse(reader);
			JsonArray jsonConnectionArr = (JsonArray) obj;
			for (JsonElement element : jsonConnectionArr) {
				parseJsonConnection(element);
			}
		} catch (FileNotFoundException e) {
			logger.log(Level.SEVERE, e.getMessage(), e);
		} catch (JsonConnectionReadException e) {
			logger.log(Level.SEVERE, e.getMessage(), e);
		}
	}

	public void parseJsonConnection(JsonElement element) throws JsonConnectionReadException {
		Gson gson = new Gson();
		JsonObject jsonObj = (JsonObject) element;
		JsonObject connectionObj = (JsonObject) jsonObj.get("Connection");
		JsonDatabase dbConn = gson.fromJson(connectionObj.toString(), JsonDatabase.class);
		if (dbConn.getIdent().isEmpty()) {
			throw new NoIdDefinedException("Id field is empty");
		} else if (dbConnections.keySet().contains(dbConn.getIdent())) {
			throw new DuplicateConnIdException("connection Id already exists");
		} else if (dbConn.getDbUrl().contains("sqlserver") && dbConn.getDbDriver().isEmpty()) {
			throw new NoDriverDefinedException("Driver must be defined for MSSQL");
		} else if (dbConn.getDbUser().isEmpty() || dbConn.getDbPassword().isEmpty()) {
			throw new NotValidUserDataException(
					"Username or Password are not defined for the Connection: " + dbConn.getIdent());
		} else {
			dbConnections.put(dbConn.getIdent(), dbConn);
		}
	}

	public Map<String, JsonDatabase> getAllJsonDatabases() {
		return dbConnections;
	}

	public JsonDatabase getJsonDatabases(String ident) {
		return dbConnections.get(ident);
	}

	public File getJasonFileConn() {
		String filePath;
		if (SystemUtils.IS_OS_WINDOWS) {
			filePath = System.getProperty("user.home") + "\\." + PROJECT_NAME + "\\dataExporter_Connections.json";
		} else {
			filePath = System.getProperty("user.home") + "/." + PROJECT_NAME + "dataExporter_Connections.json";
		}
		Path pathOfFile = Paths.get(filePath);
		return pathOfFile.toFile();
	}

	public void deleteConnections() {
		dbConnections.clear();
	}

}
