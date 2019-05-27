package de.aspera.dataexport.dao;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.SQLException;

import org.dbunit.DatabaseUnitException;
import org.junit.Test;

import de.aspera.dataexport.util.ExporterController;
import de.aspera.dataexport.util.json.ExportJsonCommand;
import de.aspera.dataexport.util.json.JsonDatabase;

public class DataSetExporterTest extends BasicFacadeTest {
	
	// TODO: Use unitils to load a dataset for using this with the current junit test.
	
	@Test
	public void exportDatasetTest() throws DatabaseUnitException, SQLException, IOException {
		JsonDatabase connectionData = new JsonDatabase();
		connectionData.setDbUrl("jdbc:mysql://127.0.0.1:3306/slc_test");
		connectionData.setDbUser("slc");
		connectionData.setDbPassword("slc1212");
		connectionData.setIdent("Ident-1");
		ExportJsonCommand command= new ExportJsonCommand();
		command.setConnId("Ident-1");
		command.setCommandId("comm-1");
		command.setExportedFilePath(".//testFolder");
		command.setTableName("sap_system");
		command.setColumns("*");
		

		ByteArrayOutputStream resultStream = ExporterController.startExportForTable(connectionData, command);
		assertNotNull("Did not create output stream!", resultStream);
		String content = resultStream.toString();
		assertTrue("Did not export the right table!", content.contains("sap_system"));
		assertTrue("Did not export the right table!", content.contains("ACTIVE"));
	}

	@Override
	public Class<?> getLoggerClass() {
		return this.getClass();
	}
}
