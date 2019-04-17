package de.aspera.locapp.util;

public class DatabaseConnectionMap {
	  private static DatabaseConnectionMap instance;

	  private DatabaseConnectionMap () {}

	  public static DatabaseConnectionMap getInstance () {
	    if (DatabaseConnectionMap.instance == null) {
	    	DatabaseConnectionMap.instance = new DatabaseConnectionMap ();
	    }
	    return DatabaseConnectionMap.instance;
	  }
	  
	  
}
