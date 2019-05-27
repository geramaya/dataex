package de.aspera.dataexport.util.json;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

public class JsonCommandHolderTest {
	private ExportJsonCommand commandJsonObj;
	private List<ExportJsonCommand> commands;

	@Before
	public void makeConnectionData() {
		commandJsonObj = new ExportJsonCommand();
		commandJsonObj.setCommandId("ID-1");
		commandJsonObj.setConnId("connId");
		commandJsonObj.setTableName("sap_sys");
		commandJsonObj.setColumns("");
		commandJsonObj.setWhereClause("x>2");
		commandJsonObj.setExportedFilePath(System.getProperty("user.dir"));
	}

	@Test(expected = ImportJsonCommandException.class)
	public void doubleCommandId() throws ImportJsonCommandException {
		ExportJsonCommandHolder holder = ExportJsonCommandHolder.getInstance();
		commands = new ArrayList<>();
		commands.add(commandJsonObj);
		commands.add(commandJsonObj);
		holder.addCommandList(commands);
	}

	@Test(expected = ImportJsonCommandException.class)
	public void emptyCommandId() throws ImportJsonCommandException {
		ExportJsonCommandHolder holder = ExportJsonCommandHolder.getInstance();
		commands = new ArrayList<>();
		commands.add(commandJsonObj);
		commandJsonObj.setCommandId("");
		commands.add(commandJsonObj);
		holder.addCommandList(commands);
	}

	@Test(expected = ImportJsonCommandException.class)
	public void emptyConnectionId() throws ImportJsonCommandException {
		ExportJsonCommandHolder holder = ExportJsonCommandHolder.getInstance();
		commands = new ArrayList<>();
		commandJsonObj.setConnId("");
		commands.add(commandJsonObj);
		holder.addCommandList(commands);
	}

	@Test(expected = ImportJsonCommandException.class)
	public void flaseFilePath() throws ImportJsonCommandException {
		ExportJsonCommandHolder holder = ExportJsonCommandHolder.getInstance();
		commands = new ArrayList<>();
		commandJsonObj.setExportedFilePath("dummy");
		;
		commands.add(commandJsonObj);
		holder.addCommandList(commands);
	}

	@Test
	public void savedCommands() throws ImportJsonCommandException {
		ExportJsonCommandHolder holder = ExportJsonCommandHolder.getInstance();
		commands = new ArrayList<>();
		ExportJsonCommand cmd = new ExportJsonCommand();
		cmd.setCommandId("ID-2");
		cmd.setConnId("connId");
		cmd.setTableName("sap_sys");
		cmd.setColumns("");
		cmd.setWhereClause("x>2");
		cmd.setExportedFilePath(System.getProperty("user.dir"));
		commands.add(commandJsonObj);
		commands.add(cmd);
		holder.addCommandList(commands);
		assertEquals(holder.getCommand("ID-1"), commandJsonObj);
		assertEquals(holder.getCommand("ID-2"), cmd);
	}

}
