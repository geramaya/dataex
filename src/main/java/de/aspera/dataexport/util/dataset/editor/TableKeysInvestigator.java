package de.aspera.dataexport.util.dataset.editor;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import de.aspera.dataexport.dataFaker.DataFaker;

public class TableKeysInvestigator {
	private DatabaseMetaData metaData;
	private Connection conn;
	private Map<String, String> keyTypeMap;
	private Map<String, Integer> numericKeyValueMap;
	private Map<String, Set<String>> strKeyValueMap;
	private static final DataFaker faker = new DataFaker();

	public void setConnection(Connection conn) {
		this.conn = conn;
		try {
			this.metaData = conn.getMetaData();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public DatabaseMetaData getMetaData() {
		return metaData;
	}

	public void setMetaData(DatabaseMetaData metaData) {
		this.metaData = metaData;
	}

	public Map<String, String> getPrimarykeysOfTable(String tableName) throws SQLException {
		if (keyTypeMap == null) {
			keyTypeMap = new HashMap<String, String>();
		}
		ResultSet keySet = metaData.getPrimaryKeys(null, null, tableName);
		while (keySet.next()) {
			String colName = keySet.getString("COLUMN_NAME");
			ResultSet column = metaData.getColumns(null, null, tableName, colName);
			column.next();
			keyTypeMap.put(tableName + "," + keySet.getString("COLUMN_NAME"),
					column.getString("TYPE_NAME") + "," + column.getString("COLUMN_SIZE"));
		}

		return keyTypeMap;
	}

	/*
	 * This Method can be replaced by finding the Max Number in the Dataset, which
	 * should contain the same Data as the DB
	 */
	private int getMaxNumberValueInColFromDB(String tableName, String colName) throws SQLException {
		Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
		ResultSet rs = stmt.executeQuery("select MAX(" + colName + ") as maxNum from " + tableName);
		if (rs.next()) {
			int result = rs.getInt("maxNum");
			stmt.close();
			return result;
		}
		return 0;
	}

	public String getValidPrimaryKeyValue(String tableName, String colName) throws SQLException {
		if (keyTypeMap == null || !keyTypeMap.containsKey(tableName + "," + colName)) {
			getPrimarykeysOfTable(tableName);
		}
		String typeOfKey = keyTypeMap.get(tableName + "," + colName);
		if (typeOfKey.toLowerCase().contains("number") || typeOfKey.toLowerCase().contains("int")) {
			return getNextValidNumber(tableName, colName);
		} else {
			return getNextValidString(tableName, colName, typeOfKey);
		}
	}

	private String getNextValidString(String tableName, String colName, String typeOfKey) throws SQLException {
		String validStr;
		Set<String> valuesSet;
		if (strKeyValueMap == null) {
			strKeyValueMap = new HashMap<String, Set<String>>();
		}
		if (!strKeyValueMap.containsKey(tableName + "," + colName)) {
			Set<String> setOfAllStrValues = getAllCharValuesInColumnFromDB(tableName, colName);
			strKeyValueMap.put(tableName + "," + colName, setOfAllStrValues);
		}
		valuesSet = strKeyValueMap.get(tableName + "," + colName);
		int strLength = Integer.parseInt(typeOfKey.split(",")[1]);
		validStr = faker.fakeStringWithLength(strLength);
		while (!valuesSet.add(validStr)) {
			// TODO: make the implementation of the faker better
			validStr = faker.fakeStringWithLength(strLength);
		}
		return validStr;
	}

	private Set<String> getAllCharValuesInColumnFromDB(String tableName, String colName) throws SQLException {
		Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
		ResultSet rs = stmt.executeQuery("select " + colName + " as charValue from " + tableName);
		Set<String> resultSet = new HashSet<String>();
		while (rs.next()) {
			String result = rs.getNString("charValue");
			resultSet.add(result);
		}
		stmt.close();
		return resultSet;
	}

	private String getNextValidNumber(String tableName, String colName) throws SQLException {
		int maxNumber;
		if (numericKeyValueMap == null) {
			numericKeyValueMap = new HashMap<String, Integer>();
		}
		if (!numericKeyValueMap.containsKey(tableName + "," + colName)) {
			maxNumber = getMaxNumberValueInColFromDB(tableName, colName);
			numericKeyValueMap.put(tableName + "," + colName, maxNumber);
		} else {
			maxNumber = numericKeyValueMap.get(tableName + "," + colName);
		}
		maxNumber++;
		numericKeyValueMap.put(tableName + "," + colName, maxNumber);
		return Integer.toString(maxNumber);
	}
	
	public List<String> getColumnNamesOfTable(String tableName) throws SQLException {
		List<String> colNames = new ArrayList<String>();
		ResultSet colNameSet = metaData.getColumns(null, null, tableName, null);
		while (colNameSet.next()) {
			colNames.add(colNameSet.getString("COLUMN_NAME"));
		}
		return colNames;
	}

}
