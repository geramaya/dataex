package de.aspera.dataexport.util.dataset.editor;

import java.sql.SQLException;
import java.util.Map;

import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;

public class DatasetRowEditor {
	private DatasetReader reader;
	private TableKeysInvestigator tableInvestigator;

	public DatasetRowEditor(DatasetReader reader) {
		this.reader = reader;
	}

	/*
	 * The Map of the new values must not have all the Column names, only the ones
	 * to be changed
	 */
	public IDataSet changeValuesInRow(String tableName, int row, Map<String, String> newValuesColName)
			throws DataSetException, DatasetReaderException, SQLException {
		Map<String, String> primaryKeys=null;
		Map<String, String> colNameValueMap = reader.getRowOfTable(tableName, row);
		if(tableInvestigator!=null) {
			primaryKeys = tableInvestigator.getPrimarykeysOfTable(tableName);
		}
		for (String colName : newValuesColName.keySet()) {
			// don't exchange values of the primary keys
			if(primaryKeys!=null) {
				//filter the primary keys out
				if (!primaryKeys.keySet().contains(tableName + "," + colName)) {
					String value = newValuesColName.get(colName);
					colNameValueMap.put(colName, value);
				}
			}else {
				// update all values without taking the primary keys into consideration
				String value = newValuesColName.get(colName);
				colNameValueMap.put(colName, value);
			}
		}
		return reader.exchangeRow(tableName, row, colNameValueMap);
	}

	public IDataSet addRow(String tableName, Map<String, String> newValuesColName)
			throws DataSetException, DatasetReaderException, SQLException {
		return reader.addRow(tableName, newValuesColName);
	}

	public void setTableKeyInvestigator(TableKeysInvestigator tableInvestigator) {
		this.tableInvestigator = tableInvestigator;

	}

}
