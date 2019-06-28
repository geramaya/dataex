package de.aspera.dataexport.cmd;

public class HelpCommand implements CommandRunnable {
	private CommandContext cmdContext;

	@Override
	public void run() {
		cmdContext = CommandContext.getInstance();
		String argument = cmdContext.nextArgument();
		if (argument == null) {
			runWithoutArguments();
		} else if (argument.toLowerCase().equals("export".toLowerCase())) {
			runHelpExportCmd();
		}

	}

	private void runHelpExportCmd() {
		System.out.println("Export command:  \n");
		System.out.println("\tTo execute an export command please use the dataExporter_connections.json and dataExporter_Exportcommands.json. \n ");
		System.out.println("\tThe connection and command files are located in [user]/.dataExporter folder.  \n ");
		System.out.println("\tThe Database form which the data will be exported must be defined in the connection file. \n");
		System.out.println("\tThe export commands must be defined in the command file. \n ");
		System.out.println("\tTo execute an export command type: export [command-Id]. \n");
		System.out.println("\tPlease refer to the examples in the command and connection files. \n");

	}

	private void runWithoutArguments() {
		System.out.println("List of commands: \n");
		System.out.println("\t(q)quit: \t\t\t\tQuit the program.");
		System.out.println("\t(e)export cmd-id\t\t\tcommand-Id of configure export commands");
		System.out.println("\tcommand options mandatory: \t\tCommand parameters without brackets are mandatory");
		System.out.println("\tcommand options optional: \t\tCommand parameters inside brackets are optional\n");
		System.out.println("\tfor more details about a command type: help [command]");
		System.out.println("\t(h)elp: \t\t\t\tPrint this!\n\n");
	}
}
