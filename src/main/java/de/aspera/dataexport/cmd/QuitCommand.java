package de.aspera.dataexport.cmd;

public class QuitCommand implements CommandRunnable {

    @Override
    public void run() {
        System.out.println("quit. good bye!");
        System.exit(0);
    }
}
