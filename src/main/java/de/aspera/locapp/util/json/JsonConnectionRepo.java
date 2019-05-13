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

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class JsonConnectionRepo {
	private static JsonConnectionRepo instance;
	public final static String PROJECT_NAME = "dataexporter";
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
		JsonParser jsonParser = new JsonParser();
		try (FileReader reader = new FileReader(getJasonFileConn())) {
			Object obj = jsonParser.parse(reader);
			JsonArray jsonConnectionArr = (JsonArray) obj;
			for (JsonElement element : jsonConnectionArr) {
				parseJsonConnection(element);
			}
		} catch (FileNotFoundException e) {
			logger.log(Level.SEVERE, e.getMessage(), e);
		} catch (IOException e) {
			logger.log(Level.SEVERE, e.getMessage(), e);
		} catch (JsonConnectionReadException e) {
			logger.log(Level.SEVERE, e.getMessage(), e);
		}
	}

	public void parseJsonConnection(JsonElement element) throws JsonConnectionReadException {
		String ident;
		String dbDriver;
		String dbUrl;
		String dbUser;
		String dbPassword;
		JsonObject jsonObj = (JsonObject) element;
		JsonObject connectionObj = (JsonObject) jsonObj.get("Connection");
		ident = connectionObj.get("ID").getAsString();
		dbDriver = connectionObj.get("Driver").getAsString();
		dbUrl = connectionObj.get("URL").getAsString();
		dbUser = connectionObj.get("Username").getAsString();
		dbPassword = connectionObj.get("Password").getAsString();
		if (ident.isEmpty()) {
			throw new NoIdDefinedException("Id field is empty");
		}
		if (dbUrl.contains("sqlserver") && dbDriver.isEmpty()) {
			throw new NoDriverDefinedException("Driver must be defined for MSSQL");
		}
		if (dbUser.isEmpty() || dbPassword.isEmpty()) {
			throw new NotValidUserDataException("Username or Password are not defined for the Connection: " + ident);
		}
		JsonDatabase connectionData = new JsonDatabase();
		connectionData.setDbDriver(dbDriver);
		connectionData.setDbPassword(dbPassword);
		connectionData.setDbUrl(dbUrl);
		connectionData.setDbUser(dbUser);
		connectionData.setIdent(ident);
		JsonDatabase insetrtedDBConn = dbConnections.put(ident, connectionData);
		if (insetrtedDBConn != null)
			throw new DuplicateConnIdException("connection Id already exists");
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
}
