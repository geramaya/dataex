package de.aspera.dataexport.util.dataset.editor;

import java.util.Map;

import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;

public class DatasetRowEditor {
	private DatasetReader reader;

	public DatasetRowEditor(DatasetReader reader) {
		this.reader = reader;
	}

	/*
	 * The Map of the new values must not have all the Column names, only the ones
	 * to be changed
	 */
	public IDataSet changeValuesInRow(String tableName, int row, Map<String, String> newValuesColName)
			throws DataSetException, DatasetReaderException {
		Map<String, String> colNameValueMap = reader.getRowOfTable(tableName, row);
		for (String name : newValuesColName.keySet()) {
			String value = newValuesColName.get(name);
			colNameValueMap.replace(name, value);
		}
		return reader.exchangeRow(tableName, row, colNameValueMap);
	}

	public IDataSet addRow(String tableName, Map<String, String> newValuesColName)
			throws DataSetException, DatasetReaderException {
		return reader.addRow(tableName, newValuesColName);
	}

}
