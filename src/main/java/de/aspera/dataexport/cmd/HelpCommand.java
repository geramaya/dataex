package de.aspera.dataexport.cmd;

public class HelpCommand implements CommandRunnable {

    @Override
    public void run() {
        System.out.println("List of commands: \n");
        System.out.println("\t(q)uit: \t\t\t\tQuit the program.");
        System.out.println("\tcommand options mandatory: \t\tCommand parameters without brackets are mandatory");
        System.out.println("\tcommand options optional: \t\tCommand parameters inside brackets are optional\n");
        System.out.println("\t(h)elp: \t\t\t\tPrint this!\n\n");
    }
}
