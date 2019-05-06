package de.aspera.locapp.dao;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.sql.SQLException;

import org.dbunit.DatabaseUnitException;
import org.junit.Before;
import org.junit.Test;

import de.aspera.locapp.util.DataConnection;
import de.aspera.locapp.util.DataSetExporter;
import de.aspera.locapp.util.ExporterController;
import de.aspera.locapp.util.TableDescriptor;

public class DataSetExporterTest extends BasicFacadeTest{
	String path;
	ExporterController exporter;
	DataConnection connectionData;
	@Before
	public void createObjects() {
		exporter = new ExporterController();
		File resourcesDirectory = new File("src/main/resources");
		path= resourcesDirectory.getAbsolutePath();
		connectionData= new DataConnection();
		connectionData.setDatabaseUrl("jdbc:mysql://127.0.0.1:3306/slc_dev");
		connectionData.setPassword("root");
		connectionData.setUsername("root");
	}
	
	@Test
	public void exportDatasetTest() throws DatabaseUnitException, SQLException {
		exporter.makeConnection(connectionData);
		exporter.startExport(path, "employee", null, null);
		//get Exported File
		File exportedFile= new File(path+"/DataSet-employee.xml.sample");
		assertNotNull("File does not exist", exportedFile );
		
	}

	@Override
	public Class<?> getLoggerClass() {
		return this.getClass();
	}

}
