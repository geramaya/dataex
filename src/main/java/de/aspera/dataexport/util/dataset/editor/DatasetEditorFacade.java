package de.aspera.dataexport.util.dataset.editor;

import java.io.FileNotFoundException;
import java.util.List;
import java.util.Map;

import org.dbunit.dataset.DataSetException;

public class DatasetEditorFacade {
	private DatasetReader reader;
	private DatasetMultiplier multiplier;
	private DatasetRowEditor rowEditor;

	public DatasetEditorFacade() {
		this.reader = new DatasetReader();
		this.multiplier = new DatasetMultiplier(reader);
		this.rowEditor = new DatasetRowEditor(reader);
	}

	public void readDataset(String filePath) throws DataSetException, FileNotFoundException, DatasetReaderException {
		reader.readDataset(filePath);
	}

	public List<String> getTabelNames() throws DataSetException {
		return reader.getTabelNames();
	}

	public int getRowCountOfTable(String tableName) {
		return reader.getRowCountOfTable(tableName);
	}

	public List<String> getColumnNamesOfTable(String tableName) throws DataSetException {
		return reader.getColumnNamesOfTable(tableName);
	}

	public String getValueInTable(String tableName, int row, String colName) throws DataSetException {
		return reader.getValueInTable(tableName, row, colName).toString();
	}

	public Map<String, String> getRowOfTable(String tableName, int row) throws DataSetException {
		return reader.getRowOfTable(tableName, row);
	}

	public void multiplyData(int factor) throws DataSetException, DatasetReaderException {
		reader.setDataset(multiplier.multiplyData(factor));
	}

	public void multiplyRowInTable(String tableName, int row, int factor)
			throws DataSetException, DatasetReaderException {
		reader.setDataset(multiplier.multiplyRowInTable(tableName, row, factor));
	}

	public void changeValuesInRow(String tableName, int row, Map<String, String> newValuesColName)
			throws DataSetException, DatasetReaderException {
		reader.setDataset(rowEditor.changeValuesInRow(tableName, row, newValuesColName));
	}

	public void addRow(String tableName, Map<String, String> newValuesColName)
			throws DataSetException, DatasetReaderException {
		reader.setDataset(rowEditor.addRow(tableName, newValuesColName));
	}

}
