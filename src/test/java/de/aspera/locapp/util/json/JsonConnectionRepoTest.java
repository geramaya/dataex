package de.aspera.locapp.util.json;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import com.google.gson.JsonObject;

public class JsonConnectionRepoTest {
	JsonObject connectionDetails;

	@Before
	public void makeConnectionData() {
		connectionDetails = new JsonObject();
		String ID = "ID-1";
		String Driver = "com.mysql.jdbc.Driver";
		String URL = "jdbc:mysql://127.0.0.1/slc_test";
		String Username = "root";
		String pass = "pass";
		connectionDetails.addProperty("ID", ID);
		connectionDetails.addProperty("Driver", Driver);
		connectionDetails.addProperty("URL", URL);
		connectionDetails.addProperty("Username", Username);
		connectionDetails.addProperty("Password", pass);
	}

	@Test(expected = NoDriverDefinedException.class)
	public void testReadJsonDbValuesThrowDriverException() throws JsonConnectionReadException {
		JsonConnectionRepo connectionRepo = JsonConnectionRepo.getInstance();
		connectionDetails.addProperty("URL", "jdbc:jtds:sqlserver://192.168.111.150:1433/slc_dev");
		connectionDetails.addProperty("Driver", "");
		JsonObject connection = new JsonObject();
		connection.add("Connection", connectionDetails);
		connectionRepo.parseJsonConnection(connection);

	}

	@Test(expected = NoIdDefinedException.class)
	public void testReadJsonDbValuesThrowIdException() throws JsonConnectionReadException {
		JsonConnectionRepo connectionRepo = JsonConnectionRepo.getInstance();
		connectionDetails.addProperty("ID", "");
		JsonObject connection = new JsonObject();
		connection.add("Connection", connectionDetails);
		connectionRepo.parseJsonConnection(connection);

	}

	@Test(expected = DuplicateConnIdException.class)
	public void testReadDatabaseThrowsItenticalIDException() throws JsonConnectionReadException {
		JsonObject connectionOne = new JsonObject();
		connectionOne.add("Connection", connectionDetails);
		JsonObject connectionTwo = new JsonObject();
		connectionTwo.add("Connection", connectionDetails);
		JsonConnectionRepo connectionRepo = JsonConnectionRepo.getInstance();
		connectionRepo.parseJsonConnection(connectionOne);
		connectionRepo.parseJsonConnection(connectionTwo);
	}

	@Test
	public void testReadJsonDbValues() throws JsonConnectionReadException {
		JsonConnectionRepo connectionRepo = JsonConnectionRepo.getInstance();
		for (int i = 0; i < 6; i++) {
			JsonObject conn = new JsonObject();
			connectionDetails.addProperty("ID", "ID-" + i);
			conn.add("Connection", connectionDetails);
			connectionRepo.parseJsonConnection(conn);
		}
		assertEquals("Wrong number of saved Connections", 6, connectionRepo.getAllJsonDatabases().size());
	}

}
