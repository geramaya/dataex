package de.aspera.dataexport.util.json;

public class TableQuery {
	private String tableName;
	private String columns;
	private String whereClaus;
	private String orderByClaus;

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getColumns() {
		return columns;
	}

	public void setColumns(String columns) {
		this.columns = columns;
	}

	public String getWhereClaus() {
		return whereClaus;
	}

	public void setWhereClaus(String whereClaus) {
		this.whereClaus = whereClaus;
	}

	public String getOrderByClaus() {
		return orderByClaus;
	}

	public void setOrderByClaus(String orderByClaus) {
		this.orderByClaus = orderByClaus;
	}
}
