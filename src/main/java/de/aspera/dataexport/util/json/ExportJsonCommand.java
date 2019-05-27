package de.aspera.dataexport.util.json;

public class ExportJsonCommand {
	private String commandId;
	private String connId;
	private String tableName;
	private String columns;
	private String whereClause;
	private String orderByClause;
	private String exportedFilePath;

	public String getCommandId() {
		return commandId;
	}

	public void setCommandId(String commandId) {
		this.commandId = commandId;
	}

	public String getConnId() {
		return connId;
	}

	public void setConnId(String connId) {
		this.connId = connId;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String table) {
		this.tableName = table;
	}

	public String getColumns() {
		return columns;
	}

	public void setColumns(String columns) {
		this.columns = columns;
	}

	public String getWhereClause() {
		return whereClause;
	}

	public void setWhereClause(String whereClause) {
		this.whereClause = whereClause;
	}

	public String getOrderByClause() {
		return orderByClause;
	}

	public void setOrderByClause(String orderByClause) {
		this.orderByClause = orderByClause;
	}

	public String getExportedFilePath() {
		return exportedFilePath;
	}

	public void setExportedFilePath(String exportedFilePath) {
		this.exportedFilePath = exportedFilePath;
	}

}
