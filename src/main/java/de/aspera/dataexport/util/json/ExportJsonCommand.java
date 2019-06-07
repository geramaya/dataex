package de.aspera.dataexport.util.json;

import java.util.List;

public class ExportJsonCommand {
	private String commandId;
	private String connId;
	private List<String> tableNames;
	private List<String> columns;
	private List<String> whereClauses;
	private List<String> orderByClauses;
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

	public List<String> getTableNames() {
		return tableNames;
	}

	public void setTableNames(List<String> table) {
		this.tableNames = table;
	}

	public List<String> getColumns() {
		return columns;
	}

	public void setColumns(List<String> columns) {
		this.columns = columns;
	}

	public List<String> getWhereClauses() {
		return whereClauses;
	}

	public void setWhereClauses(List<String> whereClauses) {
		this.whereClauses = whereClauses;
	}

	public List<String> getOrderByClauses() {
		return orderByClauses;
	}

	public void setOrderByClauses(List<String> orderByClauses) {
		this.orderByClauses = orderByClauses;
	}

	public String getExportedFilePath() {
		return exportedFilePath;
	}

	public void setExportedFilePath(String exportedFilePath) {
		this.exportedFilePath = exportedFilePath;
	}

}
