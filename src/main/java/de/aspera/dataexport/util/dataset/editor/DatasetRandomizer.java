package de.aspera.dataexport.util.dataset.editor;

import java.util.List;
import java.util.Random;

import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.DefaultDataSet;
import org.dbunit.dataset.DefaultTable;
import org.dbunit.dataset.IDataSet;

public class DatasetRandomizer {
	private DatasetReader reader;

	public DatasetRandomizer(DatasetReader reader) {
		this.reader = reader;
	}

	public IDataSet randomizeValues() throws DatasetRandomizerException {
		DefaultDataSet randomDataset = new DefaultDataSet();
		Random random = new Random();
		try {
			for (String tabName : reader.getTableNames()) {
				DefaultTable randomTable = new DefaultTable(reader.getMetaDataOfTable(tabName));
				List<String> uniques = reader.getUniqueAndPrimaryColNames(tabName);
				// Copy Old table
				for (int oldrow = 0; oldrow < reader.getRowCountOfTable(tabName); oldrow++) {
					randomTable.addRow();
					for (String colName : reader.getColumnNamesOfTable(tabName)) {
						randomTable.setValue(oldrow, colName, reader.getValueInTable(tabName, oldrow, colName));
					}
				}
				for (String colName : reader.getRandomFields(tabName)) {
					for (int row = 0; row < reader.getRowCountOfTable(tabName); row++) {
						int randomNum = random.nextInt(reader.getRowCountOfTable(tabName));
						if (!uniques.contains(colName))
							randomTable.setValue(row, colName, reader.getValueInTable(tabName, randomNum, colName));
					}
				}
				randomDataset.addTable(randomTable);
			}
		} catch (DatasetReaderException | TableKeysInvestigatorException | DataSetException e) {
			throw new DatasetRandomizerException(e.getMessage(), e);
		}
		return randomDataset;
	}

	public IDataSet randomizeValues(String tableName) throws DatasetRandomizerException {
		DefaultDataSet randomDataset = new DefaultDataSet();
		Random random = new Random();
		try {
			DefaultTable randomTable = new DefaultTable(reader.getMetaDataOfTable(tableName));
			List<String> uniques = reader.getUniqueAndPrimaryColNames(tableName);
			// Copy Old table
			for (int oldrow = 0; oldrow < reader.getRowCountOfTable(tableName); oldrow++) {
				randomTable.addRow();
				for (String colName : reader.getColumnNamesOfTable(tableName)) {
					randomTable.setValue(oldrow, colName, reader.getValueInTable(tableName, oldrow, colName));
				}
			}
			Object ahmed = null;
			for (String colName : reader.getRandomFields(tableName)) {
				for (int row = 0; row < reader.getRowCountOfTable(tableName); row++) {
					int randomNum = random.nextInt(reader.getRowCountOfTable(tableName));
					if (!uniques.contains(colName))
						ahmed = reader.getValueInTable(tableName, randomNum, colName);
					randomTable.setValue(row, colName, ahmed);
				}
			}
			randomDataset.addTable(randomTable);
			// copy old tables to the new dataset
			for (String oldTableName : reader.getTableNames()) {
				if (!oldTableName.equalsIgnoreCase(tableName)) {
					randomDataset.addTable(reader.getDataSet().getTable(oldTableName));
				}
			}
		} catch (DatasetReaderException | TableKeysInvestigatorException | DataSetException e) {
			throw new DatasetRandomizerException(e.getMessage(), e);
		}
		return randomDataset;
	}

}
