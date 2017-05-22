import java.io.RandomAccessFile;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;
import java.util.SortedMap;



public class Catalog {
	static int pageSize = 512;
	
	public static void main(String[] args) {
		initializeDataStore();
	}
		static void initializeDataStore() {

			File directory = new File("data");

			 if(!directory.exists()){
			             directory.mkdir();
			 }

	        
	        try {
	            
	            String table_file = "data/davisbase_tables.tbl";
	            File fileTemp = new File(table_file);
		        if (!fileTemp.exists()){
		            RandomAccessFile davisbaseTablesCatalog= new RandomAccessFile(table_file, "rw");
		            
			
				   davisbaseTablesCatalog.setLength(0);            
				   davisbaseTablesCatalog.setLength(pageSize);
				   davisbaseTablesCatalog.writeByte(0x0D);  
				   davisbaseTablesCatalog.writeByte(02);
				   davisbaseTablesCatalog.writeShort(0x01CF);       
					
				   davisbaseTablesCatalog.writeShort(0x01E8);   
				   davisbaseTablesCatalog.writeShort(0x01CF);   
					
	
					
				   davisbaseTablesCatalog.seek(0x1E8);			 
				   davisbaseTablesCatalog.writeShort(17);           
				   davisbaseTablesCatalog.writeInt(1);              
				   davisbaseTablesCatalog.writeByte(1);             
				   davisbaseTablesCatalog.writeByte(28);            			
				   davisbaseTablesCatalog.writeBytes("davisbase_tables");   
				   
				   
				   davisbaseTablesCatalog.seek(0x1CF);			 
				   davisbaseTablesCatalog.writeShort(18);           
				   davisbaseTablesCatalog.writeInt(2);              
				   davisbaseTablesCatalog.writeByte(1);             
				   davisbaseTablesCatalog.writeByte(29);            			
				   davisbaseTablesCatalog.writeBytes("davisbase_columns");   
				   davisbaseTablesCatalog.close();
		
		        }
	        }
	        
		catch (Exception e) {
			System.out.println("Unable to open " + "davisbase_tables");
		}
	        
	     try {
	            
	            String column_file = "data/davisbase_columns.tbl";
	            File fileTemp = new File(column_file);
		        if (!fileTemp.exists()){
		        	RandomAccessFile davisbaseColumnsCatalog= new RandomAccessFile(column_file, "rw");
	           
				
				   davisbaseColumnsCatalog.setLength(0);            
				   davisbaseColumnsCatalog.setLength(pageSize);
				   davisbaseColumnsCatalog.writeByte(0x0D);  
				   davisbaseColumnsCatalog.writeByte(9);
				   davisbaseColumnsCatalog.writeShort(0x2E);       
				
				   davisbaseColumnsCatalog.writeShort(0x01D3);   
				   davisbaseColumnsCatalog.writeShort(0x01A0);	
				   davisbaseColumnsCatalog.writeShort(0x0172);	
				   davisbaseColumnsCatalog.writeShort(0x013E);
				   davisbaseColumnsCatalog.writeShort(0x0109);
				   davisbaseColumnsCatalog.writeShort(0xD4);  
				   davisbaseColumnsCatalog.writeShort(0xA1);
				   davisbaseColumnsCatalog.writeShort(0x63);   
				   davisbaseColumnsCatalog.writeShort(0x2E);   
	
					
				   davisbaseColumnsCatalog.seek(0x1D3);			 				
				   davisbaseColumnsCatalog.writeShort(38);           			
				   davisbaseColumnsCatalog.writeInt(1);              			
				   davisbaseColumnsCatalog.writeByte(6);             			
				   davisbaseColumnsCatalog.writeByte(28);            			
				   davisbaseColumnsCatalog.writeByte(18);
				   davisbaseColumnsCatalog.writeByte(15);						
				   davisbaseColumnsCatalog.writeByte(15);						
				   davisbaseColumnsCatalog.writeByte(5);						
				   davisbaseColumnsCatalog.writeByte(14);						
				   davisbaseColumnsCatalog.writeBytes("davisbase_tables");      
				   davisbaseColumnsCatalog.writeBytes("row_id"); 			   
				   davisbaseColumnsCatalog.writeBytes("PRI"); 				    
				   davisbaseColumnsCatalog.writeBytes("INT"); 				   
				   davisbaseColumnsCatalog.writeShort(1);						
				   davisbaseColumnsCatalog.writeBytes("NO");					
				 
				   davisbaseColumnsCatalog.seek(0x1A0);			 				
				   davisbaseColumnsCatalog.writeShort(44);           			
				   davisbaseColumnsCatalog.writeInt(2);              			
				   davisbaseColumnsCatalog.writeByte(6);             			
				   davisbaseColumnsCatalog.writeByte(28);            			
				   davisbaseColumnsCatalog.writeByte(22);						
				   davisbaseColumnsCatalog.writeByte(16);						
				   davisbaseColumnsCatalog.writeByte(16);						
				   davisbaseColumnsCatalog.writeByte(5);						
				   davisbaseColumnsCatalog.writeByte(14);						
				   davisbaseColumnsCatalog.writeBytes("davisbase_tables");      
				   davisbaseColumnsCatalog.writeBytes("table_name"); 			    
				   davisbaseColumnsCatalog.writeBytes("NULL"); 				    
				   davisbaseColumnsCatalog.writeBytes("TEXT"); 				   
				   davisbaseColumnsCatalog.writeShort(2);						
				   davisbaseColumnsCatalog.writeBytes("NO");					
				   
				   davisbaseColumnsCatalog.seek(0x172);			 				
				   davisbaseColumnsCatalog.writeShort(39);           			
				   davisbaseColumnsCatalog.writeInt(3);              			
				   davisbaseColumnsCatalog.writeByte(6);             			
				   davisbaseColumnsCatalog.writeByte(29);            			
				   davisbaseColumnsCatalog.writeByte(18);						
				   davisbaseColumnsCatalog.writeByte(15);						
				   davisbaseColumnsCatalog.writeByte(15);						
				   davisbaseColumnsCatalog.writeByte(5);						
				   davisbaseColumnsCatalog.writeByte(14);						
				   davisbaseColumnsCatalog.writeBytes("davisbase_columns");      
				   davisbaseColumnsCatalog.writeBytes("row_id"); 			    
				   davisbaseColumnsCatalog.writeBytes("PRI"); 				    
				   davisbaseColumnsCatalog.writeBytes("INT"); 				   
				   davisbaseColumnsCatalog.writeShort(1);						
				   davisbaseColumnsCatalog.writeBytes("NO");					
				   
				   
				   davisbaseColumnsCatalog.seek(0x13E);			 				
				   davisbaseColumnsCatalog.writeShort(45);           			
				   davisbaseColumnsCatalog.writeInt(4);              			
				   davisbaseColumnsCatalog.writeByte(6);             			
				   davisbaseColumnsCatalog.writeByte(29);            			
				   davisbaseColumnsCatalog.writeByte(22);						
				   davisbaseColumnsCatalog.writeByte(16);						
				   davisbaseColumnsCatalog.writeByte(16);						
				   davisbaseColumnsCatalog.writeByte(5);						
				   davisbaseColumnsCatalog.writeByte(14);						
				   davisbaseColumnsCatalog.writeBytes("davisbase_columns");      
				   davisbaseColumnsCatalog.writeBytes("table_name"); 			    
				   davisbaseColumnsCatalog.writeBytes("NULL"); 				    
				   davisbaseColumnsCatalog.writeBytes("TEXT"); 				   
				   davisbaseColumnsCatalog.writeShort(2);						
				   davisbaseColumnsCatalog.writeBytes("NO");					
				   
				   
				   
				   davisbaseColumnsCatalog.seek(0x109);			 				
				   davisbaseColumnsCatalog.writeShort(46);           			
				   davisbaseColumnsCatalog.writeInt(5);              			
				   davisbaseColumnsCatalog.writeByte(6);             			
				   davisbaseColumnsCatalog.writeByte(29);            			
				   davisbaseColumnsCatalog.writeByte(23);						
				   davisbaseColumnsCatalog.writeByte(16);						
				   davisbaseColumnsCatalog.writeByte(16);						
				   davisbaseColumnsCatalog.writeByte(5);						
				   davisbaseColumnsCatalog.writeByte(14);						
				   davisbaseColumnsCatalog.writeBytes("davisbase_columns");      
				   davisbaseColumnsCatalog.writeBytes("column_name"); 			    
				   davisbaseColumnsCatalog.writeBytes("NULL"); 				    
				   davisbaseColumnsCatalog.writeBytes("TEXT"); 				   
				   davisbaseColumnsCatalog.writeShort(3);						
				   davisbaseColumnsCatalog.writeBytes("NO");					
				   
				 
				   davisbaseColumnsCatalog.seek(0xD4);			 				
				   davisbaseColumnsCatalog.writeShort(46);           			
				   davisbaseColumnsCatalog.writeInt(6);              			
				   davisbaseColumnsCatalog.writeByte(6);             			
				   davisbaseColumnsCatalog.writeByte(29);
				   davisbaseColumnsCatalog.writeByte(22);						
				   davisbaseColumnsCatalog.writeByte(16);						
				   davisbaseColumnsCatalog.writeByte(16);						
				   davisbaseColumnsCatalog.writeByte(5);						
				   davisbaseColumnsCatalog.writeByte(15);						
				   davisbaseColumnsCatalog.writeBytes("davisbase_columns");     
				   davisbaseColumnsCatalog.writeBytes("column_key");
				   davisbaseColumnsCatalog.writeBytes("NULL");
				   davisbaseColumnsCatalog.writeBytes("TEXT"); 				   
				   davisbaseColumnsCatalog.writeShort(4);						
				   davisbaseColumnsCatalog.writeBytes("YES");
				  
				   
				   /* Record rowid=7 at offset 0xA1 */
				   davisbaseColumnsCatalog.seek(0xA1);			 				
				   davisbaseColumnsCatalog.writeShort(44);           			
				   davisbaseColumnsCatalog.writeInt(7);
				   davisbaseColumnsCatalog.writeByte(6);             			
				   davisbaseColumnsCatalog.writeByte(29);            			
				   davisbaseColumnsCatalog.writeByte(21);
				   davisbaseColumnsCatalog.writeByte(16);						
				   davisbaseColumnsCatalog.writeByte(16);						
				   davisbaseColumnsCatalog.writeByte(5);
				   davisbaseColumnsCatalog.writeByte(14);						
				   davisbaseColumnsCatalog.writeBytes("davisbase_columns");      
				   davisbaseColumnsCatalog.writeBytes("data_type"); 			    
				   davisbaseColumnsCatalog.writeBytes("NULL"); 				   
				   davisbaseColumnsCatalog.writeBytes("TEXT");
				   davisbaseColumnsCatalog.writeShort(5);
				   davisbaseColumnsCatalog.writeBytes("NO");					
				   
				
				   
				   davisbaseColumnsCatalog.seek(0x63);			 				
				   davisbaseColumnsCatalog.writeShort(55);           			
				   davisbaseColumnsCatalog.writeInt(8);              			
				   davisbaseColumnsCatalog.writeByte(6);             			
				   davisbaseColumnsCatalog.writeByte(29);            			
				   davisbaseColumnsCatalog.writeByte(28);						
				   davisbaseColumnsCatalog.writeByte(16);						
				   davisbaseColumnsCatalog.writeByte(20);						
				   davisbaseColumnsCatalog.writeByte(5);						
				   davisbaseColumnsCatalog.writeByte(14);
				   davisbaseColumnsCatalog.writeBytes("davisbase_columns");      
				   davisbaseColumnsCatalog.writeBytes("ordinal_position"); 		
				   davisbaseColumnsCatalog.writeBytes("NULL"); 				    
				   davisbaseColumnsCatalog.writeBytes("SMALLINT");
				   davisbaseColumnsCatalog.writeShort(6);						
				   davisbaseColumnsCatalog.writeBytes("NO");
				   
				   
				   davisbaseColumnsCatalog.seek(0x2E);			 				
				   davisbaseColumnsCatalog.writeShort(46);           			
				   davisbaseColumnsCatalog.writeInt(9);              			
				   davisbaseColumnsCatalog.writeByte(6);             			
				   davisbaseColumnsCatalog.writeByte(29);            			
				   davisbaseColumnsCatalog.writeByte(23);						
				   davisbaseColumnsCatalog.writeByte(16);						
				   davisbaseColumnsCatalog.writeByte(16);						
				   davisbaseColumnsCatalog.writeByte(5);						
				   davisbaseColumnsCatalog.writeByte(14);						
				   davisbaseColumnsCatalog.writeBytes("davisbase_columns");      
				   davisbaseColumnsCatalog.writeBytes("is_nullable"); 			    
				   davisbaseColumnsCatalog.writeBytes("NULL"); 				    
				   davisbaseColumnsCatalog.writeBytes("TEXT"); 				   
				   davisbaseColumnsCatalog.writeShort(7);						
				   davisbaseColumnsCatalog.writeBytes("NO");					
				   
				   davisbaseColumnsCatalog.close();
		     }
	     }
	     catch (Exception e) {
				System.out.println("Unable to open " + "davisbase_columns");
			}
	    
		}
		
}