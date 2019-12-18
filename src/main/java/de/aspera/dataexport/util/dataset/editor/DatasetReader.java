package de.aspera.dataexport.util.dataset.editor;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.DefaultDataSet;
import org.dbunit.dataset.DefaultTable;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.datatype.DataType;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;

public class DatasetReader {
	private IDataSet dataset;
	private Map<String, ITable> tablesMap;
	private TableKeysInvestigator tableInvestigator;

	public void readDataset(String filePath) {
		try {
			this.dataset = new FlatXmlDataSetBuilder().build(new FileInputStream(filePath));
		} catch (DataSetException | FileNotFoundException e) {
			DatasetReaderException ex = new DatasetReaderException(e.getMessage(), e);
			ex.printStackTrace();
		}
		buildTableMap();
	}

	public List<String> getTabelNames() {
		return new ArrayList<String>(tablesMap.keySet());
	}

	public int getRowCountOfTable(String tableName) {
		return this.tablesMap.get(tableName).getRowCount();
	}

	public void setDataset(IDataSet dataset) {
		this.dataset = dataset;
		buildTableMap();
	}

	public List<String> getColumnNamesOfTable(String tableName) {
		List<String> colNames = new ArrayList<String>();
		try {
			Column[] cols = tablesMap.get(tableName).getTableMetaData().getColumns();
			for (int i = 0; i < cols.length; i++) {
				colNames.add(cols[i].getColumnName());
			}
		} catch (DataSetException e) {
			DatasetReaderException ex = new DatasetReaderException(e.getMessage(), e);
			ex.printStackTrace();
		}
		return colNames;
	}

	private void buildTableMap() {
		if (dataset == null) {
			DatasetReaderException ex = new DatasetReaderException("DataSet is null in the reader!");
			ex.printStackTrace();
		}
		tablesMap = new HashMap<String, ITable>();
		String[] tableNames;
		try {
			tableNames = dataset.getTableNames();
			for (int i = 0; i < tableNames.length; i++) {
				ITable table = dataset.getTable(tableNames[i]);
				tablesMap.put(tableNames[i], table);
			}
		} catch (DataSetException e) {
			DatasetReaderException ex = new DatasetReaderException(e.getMessage(), e);
			ex.printStackTrace();
		}

	}

	public ITableMetaData getMetaDataOfTable(String tableName) {
		return tablesMap.get(tableName).getTableMetaData();
	}

	public Object getValueInTable(String tableName, int row, String colName) {
		Object value = null;
		try {
			value = tablesMap.get(tableName).getValue(row, colName);
		} catch (DataSetException e) {
			DatasetReaderException ex = new DatasetReaderException(e.getMessage(), e);
			ex.printStackTrace();
		}
		return value;
	}

	public Map<String, String> getRowOfTable(String tableName, int row) {
		Map<String, String> colNameValueMap = new HashMap<String, String>();
		List<String> colNames = getColumnNamesOfTable(tableName);
		for (String colName : colNames) {
			colNameValueMap.put(colName, getValueInTable(tableName, row, colName).toString());
		}
		return colNameValueMap;
	}

	public IDataSet exchangeRow(String tableName, int row, Map<String, String> colNameValueMap) {
		DefaultTable table = new DefaultTable(getMetaDataOfTable(tableName));
		DefaultDataSet editedDataSet = null;
		try {
			// Copy Old table
			for (int oldrow = 0; oldrow < getRowCountOfTable(tableName); oldrow++) {
				table.addRow();
				for (String colName : getColumnNamesOfTable(tableName)) {
					table.setValue(oldrow, colName, getValueInTable(tableName, oldrow, colName));
				}
			}
			for (String colName : colNameValueMap.keySet()) {
				table.setValue(row, colName, colNameValueMap.get(colName));
			}
			editedDataSet = new DefaultDataSet();
			for (String name : getTabelNames()) {
				if (name.equals(tableName)) {
					editedDataSet.addTable(table);
				} else {
					editedDataSet.addTable(dataset.getTable(name));
				}
			}
		} catch (DataSetException e) {
			DatasetReaderException ex = new DatasetReaderException(e.getMessage(), e);
			ex.printStackTrace();
		}
		return editedDataSet;
	}

	public IDataSet addRow(String tableName, Map<String, String> newValuesColName) {
		DefaultTable table;
		DefaultDataSet editedDataSet = null;
		ITableMetaData metaData = getMetaDataOfTable(tableName);
		try {
			// the table have no columns
			if (metaData.getColumns().length == 0 && tableInvestigator != null) {
				List<String> colNames = tableInvestigator.getColumnNamesOfTable(tableName);
				Column[] cols = new Column[colNames.size()];
				for (int i = 0; i < colNames.size(); i++) {
					cols[i] = new Column(colNames.get(i), DataType.UNKNOWN);
				}
				table = new DefaultTable(tableName, cols);
			} else {
				table = new DefaultTable(metaData);
			}
			if (tableInvestigator != null) {
				Map<String, String> tableColKeys = tableInvestigator.getPrimarykeysOfTable(tableName);
				// get primary Keys of the Table and update the Values in the Map of Values
				for (String key : tableColKeys.keySet()) {
					String colName = key.split(",")[1];
					newValuesColName.put(colName, tableInvestigator.getValidPrimaryKeyValue(tableName, colName));
				}
			}

			// Copy Old table
			for (int row = 0; row < getRowCountOfTable(tableName); row++) {
				table.addRow();
				for (String colName : getColumnNamesOfTable(tableName)) {
					table.setValue(row, colName, getValueInTable(tableName, row, colName));
				}
			}
			// add new Row at the end
			int indexOfLastRow = table.getRowCount();
			table.addRow();
			for (String key : newValuesColName.keySet()) {
				table.setValue(indexOfLastRow, key, newValuesColName.get(key));
			}
			// replace the old table in the dataset
			editedDataSet = new DefaultDataSet();
			for (String name : getTabelNames()) {
				if (name.equals(tableName)) {
					editedDataSet.addTable(table);
				} else {
					editedDataSet.addTable(dataset.getTable(name));
				}
			}
		} catch (DataSetException e) {
			DatasetReaderException ex = new DatasetReaderException(e.getMessage(), e);
			ex.printStackTrace();
		}

		return editedDataSet;
	}

	public int getMaxNumberinColumnFromDataSet(String tableName, String colName) {
		int numberOfRows = getRowCountOfTable(tableName);
		int maxNum = 0;
		for (int i = 0; i < numberOfRows; i++) {
			int currentValue;
			currentValue = (int) getValueInTable(tableName, i, colName);
			if (currentValue > maxNum)
				maxNum = currentValue;
		}
		return maxNum;
	}

	public IDataSet getDataSet() {
		return dataset;
	}

	public void setTableKeyInvestigator(TableKeysInvestigator tableInvestigator) {
		this.tableInvestigator = tableInvestigator;

	}
}
