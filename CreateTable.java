

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;

public class CreateTable {
	
	public static void splitter(String command){
		String[] n = command.split("[(]");
		int j=0;
		String str=n[0];
		String sub = command.substring(str.length()+1, command.length()-1);
		String[] tokens=null;
		
		for(int k= j+1; k<n.length; k++) {
			tokens = sub.split("[,]");
		}
			
		String[] call = new String[tokens.length+1];
		call[0]=str;
		for(int y=0; y< tokens.length ;y++) {
			
			call[y+1] = tokens[y].trim();
			
		}
		createTable(call);
	}

	@SuppressWarnings("resource")
	public static void createTable(String[] strArray)
	{
		int j=0;
		String strLine="";
		String tablefilename=null;
		String tableName= null;
		try {
			RandomAccessFile columnsTableFile = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			for (int h=0; h<strArray.length;h++)   {
				strLine = strArray[h];		
				String[] tokens = strLine.split(" ");
				for(int i=0; i<tokens.length; i++)
				{
					if(tokens[i].equalsIgnoreCase("TABLE"))
					{
						tableName = tokens[i+1];
						tablefilename = tokens[i+1]+".tbl";
					}
				}  
				int len=tokens.length;
				if(tokens[0].equals("")){
					for(int u=0;u<tokens.length-1;u++)
					{
						tokens[u]=tokens[u+1];
					}
					len = tokens.length-1;
				}
				ArrayList columns = new ArrayList();
				columns.add(tableName);
															 
				
					columns.add(tokens[0]);
					if(h==1) {								
						columns.add("PRI");
						columns.add(tokens[1]);
						columns.add("NO");
					}
					if(h>1){
						if(len>2){
						
							if(tokens[2].equalsIgnoreCase("NOT")){
								columns.add("NULL");
								columns.add(tokens[1]);
								columns.add("NO");
							}
							
							else{
								columns.add("NULL");
								columns.add(tokens[1]);
								columns.add("YES");
	
									}}							
						else{
									columns.add("NULL");
									columns.add(tokens[1]);
									columns.add("YES");
								
							}
					}
					  
						String ord_pos = Integer.toString(h);
						columns.add(ord_pos);	
						String[] columns_arr = (String[]) columns.toArray(new String[columns.size()]);
						
						
					if(h!=0) {			
						Insert.insert_columns(columns_arr);
					}
			
				}
			RandomAccessFile tablesTableFile = new RandomAccessFile("data/davisbase_tables.tbl", "rw");
			Insert.insert_into_tables(tableName);
			RandomAccessFile newTable = new RandomAccessFile("data/"+tablefilename.toLowerCase(),"rw");				
			newTable.setLength(512);                           
			newTable.seek(0);
			newTable.writeByte(0x0D);
			newTable.writeByte(0);
			newTable.writeShort(512);
			
			tablesTableFile.close();
			System.out.println("Table Successfully Created");
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}