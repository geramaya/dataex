package de.aspera.locapp.cmd;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Scanner;

import com.google.gson.Gson;

import de.aspera.locapp.util.DataConnection;

/**
 * 
 * @author Victoria Schneider
 * In work.
 *
 */
public class ReadJsonFile {
	 private static ReadJsonFile instance;
     String filePath = null;
     String jsonfile = null;
     public final static String PROJECT_NAME = "dataexporter";
     static Gson gson = new Gson();
     static DataConnection data;
     
     
     // Singelton wo Jason nachher rein soll ..
	  private ReadJsonFile () {}

	  public static ReadJsonFile getInstance () {
	    if (ReadJsonFile.instance == null) {
	    	ReadJsonFile.instance = new ReadJsonFile ();
	    }
	    return ReadJsonFile.instance;
	  }
	  
	  //get File
	  public BufferedReader getFile (String filePath) throws FileNotFoundException {	  
		  // get jason File
		  BufferedReader br = new BufferedReader(new FileReader(filePath));       
		  return br; 
	  }
	  
	  
	  //Json via Gson to Object
	  public static void JsontoObject (BufferedReader br) {
		    data = gson.fromJson(br, DataConnection.class); //gilt fuer 1 Object im Jason File
		   
		    // for schleife die alles durch geht.. und neue Objekte in Array erstellt 
		    

	        // Show it.
	        System.out.println(data);
	  
	  }
	  
	  
	  //convertes to Jason after edeting in object form (jason is object)
	  public void convertesToJson() {
		  String json = gson.toJson(data);
		  
		  try {
			   //write converted json data to a file named "CountryGSON.json"
			   FileWriter writer = new FileWriter(filePath);
			   writer.write(json);
			   writer.close();
			  
			  } catch (IOException e) {
			   e.printStackTrace();
			  }
			  
			  System.out.println(json);
			  
	  }
	  
	  public void consoleInteraction() throws IOException {
		  //User can choose Filepath and decide if he wants to see oder to edit the json file ("Schnittstelle"?)
		  
		  //nach Filepath fragen
		  
		  // muss Filepath ein lesen
		  Scanner scanner = new Scanner(System.in);
		  String input = scanner.nextLine();  
		  //or
		  BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
		  String input_console = in.readLine();
		  
		  //Json ausgeben
		  
		  // 
	  }
	  
	  
}
