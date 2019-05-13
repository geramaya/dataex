package de.aspera.locapp.dao;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.SQLException;

import org.dbunit.DatabaseUnitException;
import org.junit.Test;

import de.aspera.locapp.util.DataConnection;
import de.aspera.locapp.util.ExporterController;
import de.aspera.locapp.util.json.JsonDatabase;

public class DataSetExporterTest extends BasicFacadeTest {
	
	// TODO: Use unitils to load a dataset for using this with the current junit test.
	
	@Test
	public void exportDatasetTest() throws DatabaseUnitException, SQLException, IOException {
		JsonDatabase connectionData = new JsonDatabase();
		connectionData.setDbUrl("jdbc:mysql://127.0.0.1:3306/slc_test");
		connectionData.setDbUser("user");
		connectionData.setDbPassword("password");

		ByteArrayOutputStream resultStream = ExporterController.startExportForTable(connectionData, "sap_system", "*", null, null);
		assertNotNull("Did not create output stream!", resultStream);
		String content = resultStream.toString();
		assertTrue("Did not export the right table!", content.contains("sap_system"));
	}

	@Override
	public Class<?> getLoggerClass() {
		return this.getClass();
	}
}
