package de.aspera.dataexport.util.dataset.editor;

import java.util.List;

import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.DefaultDataSet;
import org.dbunit.dataset.DefaultTable;
import org.dbunit.dataset.IDataSet;

public class DatasetMultiplier {
	private DatasetReader reader;

	public DatasetMultiplier(DatasetReader reader) {
		this.reader = reader;
	}

	public IDataSet multiplyData(int factor)
			throws TableKeysInvestigatorException, DatasetReaderException, DatasetMultiplierException {
		DefaultDataSet bigDataset = new DefaultDataSet();
		List<String> tableNames = reader.getTableNames();
		List<String> uniqesOfTable = null;
		try {
			for (String tabName : tableNames) {
				DefaultTable bigTable = new DefaultTable(reader.getMetaDataOfTable(tabName));
				// Copy Old table
				for (int oldrow = 0; oldrow < reader.getRowCountOfTable(tabName); oldrow++) {
					bigTable.addRow();
					for (String colName : reader.getColumnNamesOfTable(tabName)) {
						bigTable.setValue(oldrow, colName, reader.getValueInTable(tabName, oldrow, colName));
					}
				}
				uniqesOfTable = reader.getUniqueAndPrimaryColNames(tabName);
				for (int y = 0; y < factor; y++) {
					for (int i = 0; i < reader.getRowCountOfTable(tabName); i++) {
						List<String> colNames = reader.getColumnNamesOfTable(tabName);
						Object[] valuesOfRow = new Object[colNames.size()];
						int j = 0;
						for (String colName : colNames) {
							if (uniqesOfTable != null && uniqesOfTable.contains(colName)) {
								valuesOfRow[j] = reader.getValidUniqueKeyValue(tabName, colName);
							} else {
								valuesOfRow[j] = reader.getValueInTable(tabName, i, colName);
							}
							j++;
						}
						bigTable.addRow(valuesOfRow);
					}
				}
				bigDataset.addTable(bigTable);
			}
		} catch (DataSetException e) {
			throw new DatasetMultiplierException(e.getMessage(), e);
		}
		return bigDataset;
	}

	public IDataSet multiplyRowInTable(String tableName, int row, int factor)
			throws TableKeysInvestigatorException, DatasetReaderException, DatasetMultiplierException {
		DefaultDataSet bigDataset = new DefaultDataSet();
		DefaultTable bigTable = new DefaultTable(reader.getMetaDataOfTable(tableName));
		List<String> uniqesOfTable = null;
		try {
			// Copy Old table
			for (int oldrow = 0; oldrow < reader.getRowCountOfTable(tableName); oldrow++) {
				bigTable.addRow();
				for (String colName : reader.getColumnNamesOfTable(tableName)) {
					bigTable.setValue(oldrow, colName, reader.getValueInTable(tableName, oldrow, colName));
				}
			}
			uniqesOfTable = reader.getUniqueAndPrimaryColNames(tableName);
			for (int y = 0; y < factor; y++) {
				List<String> colNames = reader.getColumnNamesOfTable(tableName);
				Object[] valuesOfRow = new Object[colNames.size()];
				int j = 0;
				for (String colName : colNames) {
					if (uniqesOfTable != null && uniqesOfTable.contains(colName)) {
						valuesOfRow[j] = reader.getValidUniqueKeyValue(tableName, colName);
					} else {
						valuesOfRow[j] = reader.getValueInTable(tableName, row, colName);
					}
					j++;
				}
				bigTable.addRow(valuesOfRow);
			}
			bigDataset.addTable(bigTable);
			// copy other tables into the new dataset
			List<String> oldTableNames = reader.getTableNames();
			for (String oldTableName : oldTableNames) {
				if (!oldTableName.equalsIgnoreCase(tableName)) {
					bigDataset.addTable(reader.getDataSet().getTable(oldTableName));
				}
			}
		} catch (DataSetException e) {
			throw new DatasetMultiplierException(e.getMessage(), e);
		}
		return bigDataset;
	}

	public IDataSet multiplyDataInTable(String tableName, int factor)
			throws TableKeysInvestigatorException, DatasetReaderException, DatasetMultiplierException {
		DefaultDataSet bigDataset = new DefaultDataSet();
		List<String> uniqesOfTable = null;
		DefaultTable bigTable = new DefaultTable(reader.getMetaDataOfTable(tableName));
		try {
			// Copy Old table
			for (int oldrow = 0; oldrow < reader.getRowCountOfTable(tableName); oldrow++) {
				bigTable.addRow();
				for (String colName : reader.getColumnNamesOfTable(tableName)) {
					bigTable.setValue(oldrow, colName, reader.getValueInTable(tableName, oldrow, colName));
				}
			}
			uniqesOfTable = reader.getUniqueAndPrimaryColNames(tableName);
			for (int y = 0; y < factor; y++) {
				for (int i = 0; i < reader.getRowCountOfTable(tableName); i++) {
					List<String> colNames = reader.getColumnNamesOfTable(tableName);
					Object[] valuesOfRow = new Object[colNames.size()];
					int j = 0;
					for (String colName : colNames) {
						if (uniqesOfTable != null && uniqesOfTable.contains(colName)) {
							valuesOfRow[j] = reader.getValidUniqueKeyValue(tableName, colName);
						} else {
							valuesOfRow[j] = reader.getValueInTable(tableName, i, colName);
						}
						j++;
					}
					bigTable.addRow(valuesOfRow);
				}
			}
			bigDataset.addTable(bigTable);
			// copy other tables into the new dataset
			List<String> oldTableNames = reader.getTableNames();
			for (String oldTableName : oldTableNames) {
				if (!oldTableName.equalsIgnoreCase(tableName)) {
					bigDataset.addTable(reader.getDataSet().getTable(oldTableName));
				}
			}
		} catch (DataSetException e) {
			throw new DatasetMultiplierException(e.getMessage(), e);
		}
		return bigDataset;
	}
}
