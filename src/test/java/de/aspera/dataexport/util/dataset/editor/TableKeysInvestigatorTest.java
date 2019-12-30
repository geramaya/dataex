package de.aspera.dataexport.util.dataset.editor;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.DefaultDataSet;
import org.dbunit.dataset.DefaultTable;
import org.dbunit.dataset.datatype.DataType;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

@RunWith(MockitoJUnitRunner.class)
public class TableKeysInvestigatorTest {
	@Mock
	Connection conn;
	@Mock
	DatabaseMetaData metadata;
	@Mock
	ResultSet primaryKeytab1;
	@Mock
	ResultSet primaryKeytab2;
	@Mock
	ResultSet uniqueColstab1;
	@Mock
	ResultSet uniqueColstab2;
	@Mock
	ResultSet resultSetCol2ValueTab1;
	@Mock
	ResultSet resultSetCol2ValuesTab2;
	@Mock
	ResultSet resultSetCol2InfoTab1;
	@Mock
	ResultSet resultSetCol2InfoTab2;
	@Mock
	ResultSet resultSetColNamesTab1;
	@Mock
	ResultSet resultSetColNamesTab2;
	@Mock
	ResultSet resultSetColInfoTab1;
	@Mock
	ResultSet resultSetColInfoTab2;
	@Mock
	ResultSet resultSetColValueTab1;
	@Mock
	ResultSet resultSetColValuesTab2;
	@Mock
	Statement stmt;
	TableKeysInvestigator tableInvestigator;
	DatasetReader reader;
	DefaultTable table1;
	DefaultTable table2;
	DefaultDataSet dataset;

	@Before
	public void setUp() throws SQLException, DataSetException, TableKeysInvestigatorException, DatasetReaderException {
		tableInvestigator = new TableKeysInvestigator();
		reader = new DatasetReader();
		dataset = new DefaultDataSet();

		String[] values = { "1", "10" };
		Column col1 = new Column("val1Col", DataType.UNKNOWN);
		Column col2 = new Column("val2Col", DataType.UNKNOWN);
		Column[] cols = new Column[] { col1, col2 };
		table1 = new DefaultTable("test-table-1", cols);
		table1.addRow(values);
		dataset.addTable(table1);

		String[] valuestab2 = { "key1", "unique2" };
		Column col1tab2 = new Column("val1ColTab2", DataType.UNKNOWN);
		Column col2tab2 = new Column("val2ColTab2", DataType.UNKNOWN);
		Column[] colstab2 = new Column[] { col1tab2, col2tab2 };
		table2 = new DefaultTable("test-table-2", colstab2);
		table2.addRow(valuestab2);
		dataset.addTable(table2);

		Mockito.when(conn.getMetaData()).thenReturn(metadata);
		// primary Keys result set
		Mockito.when(metadata.getPrimaryKeys(null, null, "test-table-1")).thenReturn(primaryKeytab1);
		Mockito.when(metadata.getPrimaryKeys(null, null, "test-table-2")).thenReturn(primaryKeytab2);
		Mockito.when(primaryKeytab1.next()).thenAnswer(new AnswerImplementation(2));
		Mockito.when(primaryKeytab2.next()).thenAnswer(new AnswerImplementation(2));
		Mockito.when(primaryKeytab1.getString("COLUMN_NAME")).thenReturn("val1Col");
		Mockito.when(primaryKeytab2.getString("COLUMN_NAME")).thenReturn("val1ColTab2");
		Mockito.when(metadata.getColumns(null, null, "test-table-1", null)).thenReturn(resultSetColNamesTab1);
		Mockito.when(metadata.getColumns(null, null, "test-table-2", null)).thenReturn(resultSetColNamesTab2);
		Mockito.when(resultSetColNamesTab1.next()).thenAnswer(new AnswerImplementation(2));
		Mockito.when(resultSetColNamesTab2.next()).thenAnswer(new AnswerImplementation(2));
		Mockito.when(resultSetColNamesTab1.getString("COLUMN_NAME")).thenAnswer(new ColumnNamesAnswer(cols));
		Mockito.when(resultSetColNamesTab2.getString("COLUMN_NAME")).thenAnswer(new ColumnNamesAnswer(colstab2));
		Mockito.when(metadata.getColumns(null, null, "test-table-1", "val1Col")).thenReturn(resultSetColInfoTab1);
		Mockito.when(metadata.getColumns(null, null, "test-table-2", "val1ColTab2")).thenReturn(resultSetColInfoTab2);
		Mockito.when(resultSetColInfoTab1.getString("TYPE_NAME")).thenReturn("number");
		Mockito.when(resultSetColInfoTab1.getString("COLUMN_SIZE")).thenReturn("2");
		Mockito.when(resultSetColInfoTab2.getString("TYPE_NAME")).thenReturn("varchar");
		Mockito.when(resultSetColInfoTab2.getString("COLUMN_SIZE")).thenReturn("10");
		Mockito.when(conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE))
				.thenReturn(stmt);
		Mockito.when(stmt.executeQuery("select MAX(" + "val1Col" + ") as maxNum from " + "test-table-1"))
				.thenReturn(resultSetColValueTab1);
		Mockito.when(stmt.executeQuery("select " + "val1ColTab2" + " as charValue from " + "test-table-2"))
				.thenReturn(resultSetColValuesTab2);
		Mockito.when(resultSetColValueTab1.next()).thenAnswer(new AnswerImplementation(1));
		Mockito.when(resultSetColValuesTab2.next()).thenAnswer(new AnswerImplementation(1));
		Mockito.when(resultSetColValueTab1.getInt("maxNum")).thenReturn(1);
		Mockito.when(resultSetColValuesTab2.getNString("charValue")).thenReturn("key1");
		// ----------------------------------------------------------------------------------
		// unique columns result set
		// ---------------------------------------------------------------------------------
		Mockito.when(metadata.getIndexInfo(null, null, "test-table-1", true, false)).thenReturn(uniqueColstab1);
		Mockito.when(metadata.getIndexInfo(null, null, "test-table-2", true, false)).thenReturn(uniqueColstab2);
		Mockito.when(uniqueColstab1.next()).thenAnswer(new AnswerImplementation(2));
		Mockito.when(uniqueColstab2.next()).thenAnswer(new AnswerImplementation(2));
		Mockito.when(uniqueColstab1.getString("COLUMN_NAME")).thenReturn("val2Col");
		Mockito.when(uniqueColstab2.getString("COLUMN_NAME")).thenReturn("val2ColTab2");
		Mockito.when(metadata.getColumns(null, null, "test-table-1", "val2Col")).thenReturn(resultSetCol2InfoTab1);
		Mockito.when(metadata.getColumns(null, null, "test-table-2", "val2ColTab2")).thenReturn(resultSetCol2InfoTab2);
		Mockito.when(resultSetCol2InfoTab1.getString("TYPE_NAME")).thenReturn("number");
		Mockito.when(resultSetCol2InfoTab1.getString("COLUMN_SIZE")).thenReturn("2");
		Mockito.when(resultSetCol2InfoTab2.getString("TYPE_NAME")).thenReturn("varchar");
		Mockito.when(resultSetCol2InfoTab2.getString("COLUMN_SIZE")).thenReturn("10");
		Mockito.when(stmt.executeQuery("select MAX(" + "val2Col" + ") as maxNum from " + "test-table-1"))
				.thenReturn(resultSetCol2ValueTab1);
		Mockito.when(stmt.executeQuery("select " + "val2ColTab2" + " as charValue from " + "test-table-2"))
				.thenReturn(resultSetCol2ValuesTab2);
		Mockito.when(resultSetCol2ValueTab1.next()).thenAnswer(new AnswerImplementation(1));
		Mockito.when(resultSetCol2ValuesTab2.next()).thenAnswer(new AnswerImplementation(1));
		Mockito.when(resultSetCol2ValueTab1.getInt("maxNum")).thenReturn(10);
		Mockito.when(resultSetCol2ValuesTab2.getNString("charValue")).thenReturn("unique2");
		// ----------------------------------------------------------

		tableInvestigator.setConnection(conn);
		reader.setDataset(dataset);
		reader.setTableKeyInvestigator(tableInvestigator);

	}

	@Ignore
	@Test
	public void testPrimaryKeyNames() throws SQLException, TableKeysInvestigatorException {
		assertTrue("key of the first table is false",
				reader.getUniqueAndPrimaryColNames("test-table-1").contains("val1Col"));
//		assertTrue("key of the first table is false",
//				reader.getUniqueAndPrimaryColNames("test-table-1").contains("val2Col"));
		assertTrue("key of the second table is false",
				reader.getUniqueAndPrimaryColNames("test-table-2").contains("val1ColTab2"));
//		assertTrue("key of the second table is false",
//				reader.getUniqueAndPrimaryColNames("test-table-2").contains("val2ColTab2"));
	}

	@Test
	public void testValidUniqeKeyValues() throws SQLException, TableKeysInvestigatorException {
		// numeric keys primary
		String numKey = reader.getValidUniqueKeyValue("test-table-1", "val1Col");
		assertTrue("the nummeric key is not correct", numKey.equalsIgnoreCase("2"));
		numKey = reader.getValidUniqueKeyValue("test-table-1", "val1Col");
		assertFalse(numKey.equalsIgnoreCase("2"), "the second nummeric key is not correct");
		// char keys primary
		String charKey = reader.getValidUniqueKeyValue("test-table-2", "val1ColTab2");
		assertTrue("the char key length is not correct", charKey.length() == 10);
		String charKey2 = reader.getValidUniqueKeyValue("test-table-2", "val1ColTab2");
		assertNotEquals(charKey, charKey2, "the char keys are equal!");
		assertTrue("the char key length is not correct", charKey2.length() == 10);
	}

	@Ignore 
	@Test
	public void testets() throws TableKeysInvestigatorException {
		// numeric keys unique
		String numUnique = reader.getValidUniqueKeyValue("test-table-1", "val2Col");
		assertTrue("the nummeric key is not correct", numUnique.equalsIgnoreCase("11"));
		numUnique = reader.getValidUniqueKeyValue("test-table-1", "val2Col");
		assertFalse(numUnique.equalsIgnoreCase("11"), "the second nummeric key is not correct");
		// char keys unique
		String charUnique = reader.getValidUniqueKeyValue("test-table-2", "val2ColTab2");
		assertTrue("the char key length is not correct", charUnique.length() == 10);
		String charUnique2 = reader.getValidUniqueKeyValue("test-table-2", "val2ColTab2");
		assertNotEquals(charUnique, charUnique2, "the char keys are equal!");
		assertTrue("the char key length is not correct", charUnique2.length() == 10);
	}

	private final class AnswerImplementation implements Answer<Object> {
		AnswerImplementation(int maxCount) {
			this.maxCount = maxCount;
		}

		private int maxCount;
		private int count = 0;

		public Object answer(InvocationOnMock invocation) {
			if (count <= maxCount) {
				count++;
				return true;
			}
			return false;
		}
	}

	private final class ColumnNamesAnswer implements Answer<Object> {
		private int count = 0;
		private Column[] cols;

		public ColumnNamesAnswer(Column[] cols) {
			this.cols = cols;
		}

		public Object answer(InvocationOnMock invocation) {
			if (count == 0) {
				count++;
				return cols[0].getColumnName();
			} else {
				return cols[1].getColumnName();
			}
		}
	}

}
