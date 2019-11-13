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

	public DatasetReader() {
	}

	public void readDataset(String filePath) throws DataSetException, FileNotFoundException, DatasetReaderException  {
		this.dataset = new FlatXmlDataSetBuilder().build(new FileInputStream(filePath));
		buildTableMap();
	}

	public List<String> getTabelNames() throws DataSetException {
		return new ArrayList<String>(tablesMap.keySet());
	}

	public int getRowCountOfTable(String tableName) {
		return this.tablesMap.get(tableName).getRowCount();
	}

	public void setDataset(IDataSet dataset) throws Exception {
		this.dataset = dataset;
		buildTableMap();
	}

	public List<String> getColumnNamesOfTable(String tableName) throws DataSetException {
		Column[] cols = tablesMap.get(tableName).getTableMetaData().getColumns();
		List<String> colNames = new ArrayList<String>();
		for (int i = 0; i < cols.length; i++) {
			colNames.add(cols[i].getColumnName());
		}
		return colNames;
	}
	private void buildTableMap() throws DatasetReaderException, DataSetException {
		if (dataset == null) {
			throw new DatasetReaderException("Dataset is null.");
		}
		tablesMap = new HashMap<String, ITable>();
		String[] tableNames = dataset.getTableNames();
		for (int i = 0; i < tableNames.length; i++) {
			ITable table = dataset.getTable(tableNames[i]);
			tablesMap.put(tableNames[i], table);
		}
	}
	public ITableMetaData getMetaDataOfTable(String tableName) {
		return tablesMap.get(tableName).getTableMetaData();
	}
	public Object getValueInTable(String tableName, int row, String colName) throws DataSetException {
		return tablesMap.get(tableName).getValue(row, colName);
	}

}
