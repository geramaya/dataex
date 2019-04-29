package de.aspera.locapp.dao;

import java.io.File;
import java.sql.SQLException;

import org.dbunit.DatabaseUnitException;
import org.junit.Before;
import org.junit.Test;

import de.aspera.locapp.util.DataSetExporter;
import de.aspera.locapp.util.TableDescriptor;

public class DataSetExporterTest extends BasicFacadeTest{
	String path;
	String testMethod;
	TableDescriptor tableDescriptor;
	DataSetExporter exporter;
	@Before
	public void createObjects() {
		File resourcesDirectory = new File("src/main/resources/testFiles");
		path= resourcesDirectory.getAbsolutePath();
		testMethod = "DataSetExporterTest";
		tableDescriptor = new TableDescriptor("employee");
	}
	
	@Test
	public void exportDatasetTest() throws DatabaseUnitException, SQLException {
		DataSetExporter.INSTANCE.exportDataSet(path, this.getClass(), testMethod, tableDescriptor);
		
	}

	@Override
	public Class<?> getLoggerClass() {
		return this.getClass();
	}

}
