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
	ResultSet resultSetCharKeyValuesTab2;
	@Mock
	Statement stmt;
	TableKeysInvestigator tableInvestigator;
	DefaultTable table1;
	DefaultTable table2;
	DefaultDataSet dataset;

	@Before
	public void setUp() throws SQLException, DataSetException, TableKeysInvestigatorException {
		tableInvestigator = new TableKeysInvestigator();
		dataset = new DefaultDataSet();
		Mockito.when(conn.getMetaData()).thenReturn(metadata);
		// first Table Primary key is number
		Mockito.when(resultSetColNamesTab1.next()).thenAnswer(new AnswerImplementation());
		Mockito.when(resultSetColInfoTab1.next()).thenReturn(true);
		Mockito.when(resultSetColValueTab1.next()).thenReturn(true);
		Mockito.when(resultSetColValueTab1.getInt("maxNum")).thenReturn(1);
		Mockito.when(resultSetColNamesTab1.getString("COLUMN_NAME")).thenReturn("val1Col");
		Mockito.when(resultSetColInfoTab1.getString("TYPE_NAME")).thenReturn("BigInt");
		Mockito.when(resultSetColInfoTab1.getString("COLUMN_SIZE")).thenReturn("10");
		Mockito.when(metadata.getPrimaryKeys(null, null, "test-tab")).thenReturn(resultSetColNamesTab1);
		Mockito.when(metadata.getColumns(null, null, "test-tab", "val1Col")).thenReturn(resultSetColInfoTab1);
		Mockito.when(stmt.executeQuery("select MAX(" + "val1Col" + ") as maxNum from " + "test-tab"))
				.thenReturn(resultSetColValueTab1);
		Mockito.when(conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE))
				.thenReturn(stmt);

		String[] values = { "1", "val2", "val3" };
		Column col1 = new Column("val1Col", DataType.UNKNOWN);
		Column col2 = new Column("val2Col", DataType.UNKNOWN);
		Column col3 = new Column("val3Col", DataType.UNKNOWN);
		Column[] cols = new Column[] { col1, col2, col3 };
		table1 = new DefaultTable("test-table", cols);
		table1.addRow(values);
		dataset.addTable(table1);

		// second table primary key is char
		Mockito.when(resultSetColNamesTab2.next()).thenAnswer(new AnswerImplementation());
		Mockito.when(resultSetColInfoTab2.next()).thenReturn(true);
		Mockito.when(resultSetColNamesTab2.getString("COLUMN_NAME")).thenReturn("val1ColTab2");
		Mockito.when(resultSetColInfoTab2.getString("TYPE_NAME")).thenReturn("Varchar");
		Mockito.when(resultSetColInfoTab2.getString("COLUMN_SIZE")).thenReturn("10");
		Mockito.when(stmt.executeQuery("select val1ColTab2 as charValue from test-tab-2"))
				.thenReturn(resultSetCharKeyValuesTab2);
		Mockito.when(resultSetCharKeyValuesTab2.next()).thenAnswer(new AnswerImplementation());
		Mockito.when(resultSetCharKeyValuesTab2.getNString("charValue")).thenReturn("key1");
		Mockito.when(metadata.getPrimaryKeys(null, null, "test-tab-2")).thenReturn(resultSetColNamesTab2);
		Mockito.when(metadata.getColumns(null, null, "test-tab-2", "val1ColTab2")).thenReturn(resultSetColInfoTab2);

		String[] valuestab2 = { "key1", "val2", "val3" };
		Column col1tab2 = new Column("val1ColTab2", DataType.UNKNOWN);
		Column col2tab2 = new Column("val2ColTab2", DataType.UNKNOWN);
		Column col3tab2 = new Column("val3ColTab2", DataType.UNKNOWN);
		Column[] colstab2 = new Column[] { col1tab2, col2tab2, col3tab2 };
		table2 = new DefaultTable("test-table-2", colstab2);
		table2.addRow(valuestab2);
		tableInvestigator.setConnection(conn);
	}

	@Test
	public void testPrimaryKeyNames() throws SQLException, TableKeysInvestigatorException {
		assertTrue("key of the first table is false",
				tableInvestigator.getPrimarykeysOfTable("test-tab").containsKey("test-tab,val1Col"));
		assertTrue("key of the second table is false",
				tableInvestigator.getPrimarykeysOfTable("test-tab-2").containsKey("test-tab-2,val1ColTab2"));
	}

	@Test
	public void testValidPrimaryKeyValues() throws SQLException, TableKeysInvestigatorException {
		// nummeric keys
		String numKey = tableInvestigator.getValidPrimaryKeyValue("test-tab", "val1Col");
		assertTrue("the nummeric key is not correct", numKey.equalsIgnoreCase("2"));
		numKey = tableInvestigator.getValidPrimaryKeyValue("test-tab", "val1Col");
		assertFalse(numKey.equalsIgnoreCase("2"), "the second nummeric key is not correct");
		// char keys
		String charKey = tableInvestigator.getValidPrimaryKeyValue("test-tab-2", "val1ColTab2");
		assertTrue("the char key length is not correct", charKey.length() == 10);
		String charKey2 = tableInvestigator.getValidPrimaryKeyValue("test-tab-2", "val1ColTab2");
		assertNotEquals(charKey, charKey2, "the char keys are equal!");
		assertTrue("the char key length is not correct", charKey2.length() == 10);
	}

	private final class AnswerImplementation implements Answer<Object> {
		private int count = 0;

		public Object answer(InvocationOnMock invocation) {
			if (count == 0) {
				count++;
				return true;
			}
			return false;
		}
	}

}
