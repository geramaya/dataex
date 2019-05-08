package de.aspera.locapp.util.json;


//Importe bitte drin lassen, Code ist nur ausgeklammert
import java.lang.reflect.Type;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;

import de.aspera.locapp.util.DataConnection;

/**
 * 
 * @author Victoria Schneider
 * In work.
 *
 */
public class ReadJsonFile {
	 private static ReadJsonFile instance;
     String jsonfile = null;
     public final static String PROJECT_NAME = "dataexporter";
     static Gson gson = new Gson();
     static JsonDatabase data;
     
     
     // Singelton wo Jason nachher rein soll ..
	  private ReadJsonFile () {}

	  public static ReadJsonFile getInstance () {
	    if (ReadJsonFile.instance == null) {
	    	ReadJsonFile.instance = new ReadJsonFile ();
	    }
	    return ReadJsonFile.instance;
	  }
	  
	  //get File
	  public static BufferedReader getFile (String filePath) throws FileNotFoundException {	  

		  BufferedReader br = new BufferedReader(new FileReader(filePath));       
		  return br; 
	  }
	  
	  
	  //Json via Gson to Object
	  public static void JsontoObject (BufferedReader file) {
		  //TODO: siehe unten
		  
		  	file.toString();
		  
		    //data = gson.fromJson(br, JsonDatabase.class); //gilt fuer 1 Object im Jason File
		   
		    // for schleife die alles durch geht.. und neue Objekte in Array erstellt 
		    
		  	/* JsonObject obj = json.getAsJsonObject();
		    
		    
		    JsonObject associative = getAsJsonObject("associative.json");
		    JsonObject associativeData = associative.getAsJsonObject("data"); 

		    ArrayList<JsonArray> listA = new ArrayList<JsonArray>(); 

		    

		    JsonObject array = xx.getAsJsonObject("array.json");
		    JsonArray arrayData = array.getAsJsonArray("data");
		    
		    for(int i = 0 ; i < arrayData.size(); i++){
		    	JsonObject data = arrayData.getAsJsonObject(i);
		    	
		        JsonArray synonyms = data.getAsJsonArray("synonyms");

		        } */
		    
		    // Hashmap? Vill gute Idee vill nicht, wird geprüft -> recherche ueber Hashmaps laeuft
		    
		    Type jsonDatabaseType =  new TypeToken<HashMap<String, JsonDatabase>>(){}.getType();

		    HashMap<String, JsonDatabase> jsonHashmap = gson.fromJson(file, jsonDatabaseType);

	        // Show it.
	        //System.out.println(data);
	  
	  }
	  
	  


	//convertes to Jason after edeting in object form (jason is object)
	  public void convertesToJson(String objects, String filePathWhereTo) {
		  //TODO ueberpruefen -> evt andere Methode (Seite gespeichert)
		  String json = gson.toJson(data);
		  
		  try {
			   //write converted json data to a file named "CountryGSON.json"
			   FileWriter writer = new FileWriter(filePathWhereTo);
			   writer.write(json);
			   writer.close();
			  
			  } catch (IOException e) {
			   e.printStackTrace();
			  }
			  
			  System.out.println(json);
			  
	  }
	
	  
	  //User can choose Filepath and decide if he wants to see oder to edit the json file ("Schnittstelle"?)
	  public String consoleInteractionAskForFilepath() throws IOException {

		  //ask about the Filepath 
		  System.out.println("Geben Sie den FilePath ein!");
		  
		  // read FilePath
		  @SuppressWarnings("resource")
		Scanner scanner = new Scanner(System.in);
		  String filePath = scanner.next(); 
		  	
		  return filePath;
	  }
	  
	  
	  //User can choose if he wants to edit the json file ("Schnittstelle"?) 
	  public boolean consoleInteractionAskForEditing() throws IOException {

		  //nach edit fragen
		  System.out.println("Wollen Sie die Datei bearbeiten? Geben Sie einen boolischen Wert ('true' = ja; 'false' = nein) ein.");
		  
		  // muss Filepath ein lesen
		  @SuppressWarnings("resource")
		Scanner scanner = new Scanner(System.in); 
		  boolean answer = scanner.nextBoolean();    

		  return answer;	  
	  }
	  
	  
		//edites Objects (former Json) 
	  public String editesObjects(String objects) {
		  //TODO implemend method
			  return objects;
	  }
	
	  
	  
	  
	  public void main() throws IOException {
		  //get Path
		  String filePathMain = consoleInteractionAskForFilepath();
		  
		  //get File
		  BufferedReader file = getFile(filePathMain);
		  
		  //convert Json to Object (maybe Hashmap)
		  JsontoObject(file);
		  
		  //ask if Objects should be edited
		  if (consoleInteractionAskForEditing() == true) { 
			  editesObjects("(objects from Jason, which should be edited)");
			  }	  
		  
		  //convertes Objects to Json
		  convertesToJson("(objects from Jason, which may are edited)" , "(File Path Where to)");
	  
	  }	  
	  
}
