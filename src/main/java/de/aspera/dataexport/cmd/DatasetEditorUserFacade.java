package de.aspera.dataexport.cmd;

import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.util.List;
import java.util.Map;

import org.dbunit.dataset.IDataSet;

import de.aspera.dataexport.util.dataset.editor.DatasetEditorException;
import de.aspera.dataexport.util.dataset.editor.DatasetEditorFacade;
import de.aspera.dataexport.util.dataset.editor.DatasetMultiplierException;
import de.aspera.dataexport.util.dataset.editor.DatasetRandomizerException;
import de.aspera.dataexport.util.dataset.editor.DatasetReaderException;
import de.aspera.dataexport.util.dataset.editor.DatasetRowEditorException;
import de.aspera.dataexport.util.dataset.editor.TableKeysInvestigatorException;

public class DatasetEditorUserFacade {
	private static DatasetEditorFacade facade;

	public DatasetEditorUserFacade(ByteArrayInputStream inputStream)
			throws DatasetEditorException, DatasetReaderException {
		facade = new DatasetEditorFacade();
		facade.readDataset(inputStream);
	}

	public static void multiplyData(int factor)
			throws TableKeysInvestigatorException, DatasetReaderException, DatasetMultiplierException {
		facade.multiplyData(factor);
	}

	public static void multiplyRowInTable(String tableName, int row, int factor)
			throws TableKeysInvestigatorException, DatasetReaderException, DatasetMultiplierException {
		facade.multiplyRowInTable(tableName, row, factor);
	}

	public static void multiplyDataInTable(String tableName, int factor)
			throws TableKeysInvestigatorException, DatasetReaderException, DatasetMultiplierException {
		facade.multiplyDataInTable(tableName, factor);
	}

	public static void changeValuesInRow(String tableName, int row, Map<String, String> newValuesColName)
			throws DatasetReaderException, TableKeysInvestigatorException, DatasetRowEditorException {
		facade.changeValuesInRow(tableName, row, newValuesColName);
	}

	public static void addRow(String tableName, Map<String, String> newValuesColName)
			throws TableKeysInvestigatorException, DatasetReaderException, DatasetRowEditorException {
		facade.addRow(tableName, newValuesColName);
	}

	public static void setRandomFields(List<String> fields) {
		facade.setRandomFields(fields);
	}

	public static void randomizeValues(boolean keepOldData) throws DatasetReaderException, DatasetRandomizerException {
		facade.randomizeValues(keepOldData);
	}

	public static void randomizeValues(String tableName, boolean keepOldData)
			throws DatasetReaderException, DatasetRandomizerException {
		facade.randomizeValues(tableName, keepOldData);
	}

	protected void setConnectionOfDB(Connection connection) throws TableKeysInvestigatorException {
		facade.setConnectionOfDB(connection);

	}

	protected IDataSet getDataSet() {
		return facade.getDataSet();
	}

}
