package de.aspera.locapp.util.json;

import java.util.Set;

public class JsonConnectionRepo {
	  private static JsonConnectionRepo instance;

	  private JsonConnectionRepo () {}

	  /**
	   * This method create and returns the singleton instance.
	   * @return
	   */
	  public static JsonConnectionRepo getInstance () {
	    if (instance == null) {
	    	instance = new JsonConnectionRepo ();
	    	instance.initJsonDatabases();
	    }
	    return instance;
	  }

	private void initJsonDatabases() {
		// TODO: read json data from file and hold this connections as list.
		// TODO: The ident must be unique for a dbconnection object
	}
	
	public Set<JsonDatabase> getAllJsonDatabases() {
		return null;
	}
	
	public JsonDatabase getJsonDatabases(String ident) {
		return null;
	}
	
}
