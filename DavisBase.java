import java.io.RandomAccessFile;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.*;
import java.util.Scanner;
import java.util.TreeMap;

@SuppressWarnings("unused")
public class DavisBase {

	static String prompt = "DavisSql> ";
	static int page_size = 512;
	@SuppressWarnings("resource")
	public static void main(String[] args) {
		
		SplashScreen.main(args);
		Catalog.main(args);
		Scanner scanner = new Scanner(System.in).useDelimiter(";");
		String userCommand; // Variable to collect user input from the prompt

		do { 
			System.out.print(prompt);
			userCommand = scanner.next().trim();
			String[] cmd = userCommand.split(" ");

			if (userCommand.equalsIgnoreCase("SHOW TABLES")) {
				TableUtils.showTables();
			}
			else if (cmd[0].equalsIgnoreCase("INSERT")) {
				InsertData.insert(userCommand);
			} 
			else if (cmd[0].equalsIgnoreCase("CREATE")) {
				CreateTable.splitter(userCommand);
			}
			else if (cmd[0].equalsIgnoreCase("SELECT")) {
				SelectData.selectall(userCommand);
			} 
			else if (cmd[0].equalsIgnoreCase("DROP")) {
				TableUtils.drop_table(userCommand);
			}
			else if (cmd[0].equalsIgnoreCase("UPDATE")) {
				if (userCommand.contains("WHERE") || userCommand.contains("where")) {
					UpdateData.updatemethod(userCommand);
				} else {
					System.out.println("Where condition not specified");
				}	
			} 
			else if (userCommand.equalsIgnoreCase("help")) {
				System.out.println("help tables");
				help();
			}
			else if (userCommand.equalsIgnoreCase("exit")) {
				System.out.println("exit tables");
			} 
			else {
				System.out.println("i didnt get command");
			}
		} 
		while (!userCommand.equals("exit"));
		System.out.println("Exiting...");
	} 
	
	
	public static void help() {
		System.out.println(line("*", 80));
		System.out.println();
		System.out.println("\tSHOW TABLES		Displays all tables");
		System.out.println("\tCREATE TABLE	 	Creates a new table schema, i.e. a new empty table");
		System.out.println("\tINSERT INTO TABLE 	Inserts a row/record into a table");
		System.out.println("\tSELECT-FROM-WHERE 	style query");
		System.out.println("\tUPDATE TABLE 	    update table set where <condition>");
		System.out.println("\tDROP TABLE 	    DROPs given table");
		System.out.println("\thelp;          		Show this help information");
		System.out.println("\texit;          		Exit the program");
		System.out.println();
		System.out.println();
		System.out.println(line("*", 80));
	}

	public static String line(String s, int num) {
		String a = "";
		for (int i = 0; i < num; i++) {
			a += s;
		}
		return a;
	}
}


