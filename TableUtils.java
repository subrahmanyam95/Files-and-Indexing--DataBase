

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;

public class TableUtils {
	static int page_size = 512;
	
	
	@SuppressWarnings("resource")
	public static void drop_table(String userCommand) {
		try {
			
			String[] token = userCommand.split(" ");
			String table_name =token[2];	
			String filename = "data/"+table_name.toLowerCase()+".tbl";
			
	        
	         File fileTemp = new File(filename);
	          if (fileTemp.exists()){
	             fileTemp.delete();    	
	             set_null(table_name); 
	             System.out.println("Table Deleted");
				}
	          else {
	        	  System.out.println("No such Table");
	          }
	          
			}
		catch (Exception e) {
			e.printStackTrace();
		}
	
	}
	
	
	private static void set_null(String table_name) throws IOException{
		
		String table_file = "data/davisbase_tables.tbl";
		RandomAccessFile TableFile = new RandomAccessFile(table_file, "rw");
		File f = new File(table_file);
        long fileLength = f.length();
        int len = (int) fileLength;
        
        int no_of_pages = len/page_size;
        for(int y=0;y<no_of_pages;y++) {
        	int page_start=y*512;
        	TableFile.seek(page_start);
           int type = TableFile.readByte();
           if(type == 13) {
        	   TableFile.seek(page_start+1);
        	   int row_count = TableFile.readByte();
        	   TableFile.seek(page_start+4);
        	   int[] array = new int[row_count];
        	   for(int i=0;i<row_count;i++) {
        		   array[i] = TableFile.readShort();
        		   
        	   }
        	  
        	   for(int j=0;j<array.length;j++) {
        		   if(array[j]!=1){
	        	       TableFile.seek(array[j]+6);
	        		   int no_columns = TableFile.readByte();
	        		   TableFile.seek(array[j]+7);
	        		   int table_name_len = TableFile.readByte()-12;
	        		   TableFile.seek(array[j]+8+no_columns-1);
	        		   String str = "";
	        		   for(int l=0;l<table_name_len;l++) {
	        			   str += (char) TableFile.readByte();
	        		   }
	        		   if(str.equalsIgnoreCase(table_name)) {
	        			   TableFile.seek(page_start+4+2*(j));
	        			   TableFile.writeShort(0x01);
	        			   System.out.println();
        		   }
        		   }
        	   }
        	   
           }
        }
		
        String column_file = "data/davisbase_columns.tbl";
		RandomAccessFile ColumnFile = new RandomAccessFile(column_file, "rw");
		File n = new File(column_file);
        long columnLength = n.length();
        int len_col = (int) columnLength;
        
        int no_pages = len_col/page_size;
        
       int x=0;
        while(x<no_pages) {
        	int page_start=x*512;
        	ColumnFile.seek(page_start);
           int type = ColumnFile.readByte();
           if(type == 13) {
        	   ColumnFile.seek(page_start+1);
        	   int row_count = ColumnFile.readByte();
        	   ColumnFile.seek(page_start+4);
        	   int[] array = new int[row_count];
        	   for(int i=0;i<row_count;i++) {
        		   array[i] = ColumnFile.readShort();
        		   
        	   }
     	   
        	   for(int j=0;j<array.length;j++) {
        		   if(array[j]!=1){
	        		   ColumnFile.seek(array[j]+6);
	        		   int no_columns = ColumnFile.readByte();
	        		   ColumnFile.seek(array[j]+7);
	        		   
	        		   int name_len = ColumnFile.readByte()-12;
	        		   ColumnFile.seek(array[j]+8+no_columns-1);
	        		   
	        		   String str="";
	        		   for(int p=0;p<name_len;p++) {
	        			   str+= (char) ColumnFile.readByte();
	        			   
	        		   }
	        		   if(str.equalsIgnoreCase(table_name)) {
	        			   ColumnFile.seek(page_start+4+2*(j));
	        			   ColumnFile.writeShort(0x01);
        			  
        		    }
        		   }
        	   }
        	   
           }
           x++;
          
           
        }
        
	}
	public static void showTables() {
		String userCommand="Select * from davisbase_tables";
		SelectData.selectall(userCommand);
	}

}