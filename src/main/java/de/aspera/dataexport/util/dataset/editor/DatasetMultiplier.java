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

	public IDataSet multiplyData(int factor) throws DataSetException {
		DefaultDataSet bigDataset = new DefaultDataSet();
		List<String> tableNames = reader.getTabelNames();
		for (String tabName : tableNames) {
			DefaultTable bigTable = new DefaultTable(reader.getMetaDataOfTable(tabName));
			for (int y = 0; y < factor; y++) {
				for (int i = 0; i < reader.getRowCountOfTable(tabName); i++) {
					List<String> colNames = reader.getColumnNamesOfTable(tabName);
					Object[] valuesOfRow = new Object[colNames.size()];
					int j = 0;
					for (String colName : colNames) {
						valuesOfRow[j] = reader.getValueInTable(tabName, i, colName);
						j++;
					}
					bigTable.addRow(valuesOfRow);
				}
			}
			bigDataset.addTable(bigTable);
		}
		return bigDataset;
	}
}
