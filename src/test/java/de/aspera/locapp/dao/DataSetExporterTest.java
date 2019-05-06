package de.aspera.locapp.dao;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.SQLException;

import org.dbunit.DatabaseUnitException;
import org.junit.Before;
import org.junit.Test;

import de.aspera.locapp.util.DataConnection;
import de.aspera.locapp.util.ExporterController;

public class DataSetExporterTest extends BasicFacadeTest{
	private ExporterController exporter;
	private DataConnection connectionData;
	private ByteArrayOutputStream resultStream;
	@Before
	public void createObjects() {
		exporter = new ExporterController();
		connectionData= new DataConnection();
		connectionData.setDatabaseUrl("jdbc:mysql://127.0.0.1:3306/slc_dev");
		connectionData.setPassword("root");
		connectionData.setUsername("root");
	}
	
	@Test
	public void exportDatasetTest() throws DatabaseUnitException, SQLException, IOException {
		exporter.makeConnection(connectionData);
		resultStream = exporter.startExport("sap_system", null, null);
		assertNotNull("Did not create output stream!", resultStream );
		String content = resultStream.toString();
		assertTrue("Did not export the right table!", content.contains("sap_system"));
		
	}

	@Override
	public Class<?> getLoggerClass() {
		return this.getClass();
	}

}
