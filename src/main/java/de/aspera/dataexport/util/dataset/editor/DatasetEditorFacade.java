package de.aspera.dataexport.util.dataset.editor;

import java.io.InputStream;
import java.sql.Connection;
import java.util.List;
import java.util.Map;

import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;

public class DatasetEditorFacade {
	private DatasetReader reader;
	private DatasetMultiplier multiplier;
	private DatasetRowEditor rowEditor;
	private TableKeysInvestigator tableInvestigator;

	public DatasetEditorFacade() {
		this.reader = new DatasetReader();
		this.multiplier = new DatasetMultiplier(reader);
		this.rowEditor = new DatasetRowEditor(reader);
		this.tableInvestigator = new TableKeysInvestigator();
	}

	public void readDataset(String filePath) {
		reader.readDataset(filePath);
	}

	public void readDataset(InputStream stream) {
		IDataSet dataset;
		try {
			dataset = new FlatXmlDataSetBuilder().build(stream);
			reader.setDataset(dataset);
		} catch (DataSetException e) {
			DatasetEditorException ex = new DatasetEditorException(e.getMessage(), e);
			ex.printStackTrace();
		}
	}

	public List<String> getTabelNames() {
		return reader.getTabelNames();
	}

	public int getRowCountOfTable(String tableName) {
		return reader.getRowCountOfTable(tableName);
	}

	public List<String> getColumnNamesOfTable(String tableName) {
		return reader.getColumnNamesOfTable(tableName);
	}

	public String getValueInTable(String tableName, int row, String colName) {
		return reader.getValueInTable(tableName, row, colName).toString();
	}

	public Map<String, String> getRowOfTable(String tableName, int row) {
		return reader.getRowOfTable(tableName, row);
	}

	public void multiplyData(int factor) {
		reader.setDataset(multiplier.multiplyData(factor));
	}

	public void multiplyRowInTable(String tableName, int row, int factor) {
		reader.setDataset(multiplier.multiplyRowInTable(tableName, row, factor));
	}

	public void multiplyDataInTable(String tableName, int factor){
		reader.setDataset(multiplier.multiplyDataInTable(tableName, factor));
	}

	public void changeValuesInRow(String tableName, int row, Map<String, String> newValuesColName) {
		reader.setDataset(rowEditor.changeValuesInRow(tableName, row, newValuesColName));
	}

	public void addRow(String tableName, Map<String, String> newValuesColName){
		reader.setDataset(rowEditor.addRow(tableName, newValuesColName));
	}

	public void setConnectionOfDB(Connection conn) {
		tableInvestigator.setConnection(conn);
		reader.setTableKeyInvestigator(tableInvestigator);
		rowEditor.setTableKeyInvestigator(tableInvestigator);
		multiplier.setTableKeyInvestigator(tableInvestigator);
	}

	public IDataSet getDataSet() {
		return reader.getDataSet();
	}

}
