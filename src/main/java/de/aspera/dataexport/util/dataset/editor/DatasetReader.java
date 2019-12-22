package de.aspera.dataexport.util.dataset.editor;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;

public class DatasetReader {
	private IDataSet dataset;
	private Map<String, ITable> tablesMap;
	private TableKeysInvestigator tableInvestigator;
	private Map<String, TableConstrainsDescription> tablesConstraints;

	public void readDataset(String filePath) throws DatasetReaderException {
		try {
			this.dataset = new FlatXmlDataSetBuilder().build(new FileInputStream(filePath));
		} catch (DataSetException | FileNotFoundException e) {
			throw new DatasetReaderException(e.getMessage(), e);
		}
		buildTableMap();
	}

	public List<String> getTableNames() {
		return new ArrayList<String>(tablesMap.keySet());
	}

	public int getRowCountOfTable(String tableName) {
		return this.tablesMap.get(tableName).getRowCount();
	}

	public void setDataset(IDataSet dataset) throws DatasetReaderException {
		this.dataset = dataset;
		buildTableMap();
	}

	public List<String> getColumnNamesOfTable(String tableName) throws DatasetReaderException {
		List<String> colNames = new ArrayList<String>();
		try {
			Column[] cols = tablesMap.get(tableName).getTableMetaData().getColumns();
			for (int i = 0; i < cols.length; i++) {
				colNames.add(cols[i].getColumnName());
			}
		} catch (DataSetException e) {
			throw new DatasetReaderException(e.getMessage(), e);
		}
		return colNames;
	}

	private void buildTableMap() throws DatasetReaderException {
		if (dataset == null) {
			throw new DatasetReaderException("DataSet is null in the reader!");
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
			throw new DatasetReaderException(e.getMessage(), e);
		}

	}

	public ITableMetaData getMetaDataOfTable(String tableName) {
		return tablesMap.get(tableName).getTableMetaData();
	}

	public Object getValueInTable(String tableName, int row, String colName) throws DatasetReaderException {
		Object value = null;
		try {
			value = tablesMap.get(tableName).getValue(row, colName);
		} catch (DataSetException e) {
			throw new DatasetReaderException(e.getMessage(), e);
		}
		return value;
	}

	public Map<String, String> getRowOfTable(String tableName, int row) throws DatasetReaderException {
		Map<String, String> colNameValueMap = new HashMap<String, String>();
		List<String> colNames = getColumnNamesOfTable(tableName);
		for (String colName : colNames) {
			colNameValueMap.put(colName, getValueInTable(tableName, row, colName).toString());
		}
		return colNameValueMap;
	}

	public int getMaxNumberinColumnFromDataSet(String tableName, String colName) throws DatasetReaderException {
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

	public void setTableKeyInvestigator(TableKeysInvestigator tableInvestigator) throws TableKeysInvestigatorException {
		this.tableInvestigator = tableInvestigator;
		this.tablesConstraints = tableInvestigator.createTableConstrainsDescriptions(getTableNames());
	}

	public String getValidUniqueKeyValue(String tabName, String colName) throws TableKeysInvestigatorException {
		return tablesConstraints.get(tabName).getValidUniqueKeyValue(colName);
	}

	// will be useful when using foreign keys
	public List<String> getPrimarykeysOfTable(String tableName) throws TableKeysInvestigatorException {
		if (tableInvestigator == null) {
			throw new TableKeysInvestigatorException("table investigator is null!");
		}
		return tablesConstraints.get(tableName).getPrimaryKeyNames();
	}

	public List<String> getUniqueAndPrimaryColNames(String tableName) throws TableKeysInvestigatorException {
		if (tableInvestigator == null) {
			return null;
		}
		return tablesConstraints.get(tableName).getUniqueAndPrimaryColNames();
	}

	public boolean isTableInvestigatorNull() {
		if (tableInvestigator == null)
			return true;
		else
			return false;
	}

	public List<String> getColNamesOfTable(String tableName) {
		return tablesConstraints.get(tableName).getColNames();
	}
}
