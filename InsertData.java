import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map.Entry;

public class InsertData {
	static int page_size = 512;
	
	
	public static void insert(String command){              
	   try {
			command = command.trim();
			String[] names=command.split("[(]");			
			String[] tableN = names[0].split(" ");
			String tableName = tableN[2];
			String[] value_q;
			if(names.length==3) {
			    value_q = names[2].split("[,)]");
			}
			else {
				value_q = names[1].split("[,)]");
			}
			String[] value = new String[value_q.length];
						
			for(int x=0; x<value.length;x++) {
				String str="";															
				String new_val = value_q[x].trim();
				if(new_val.contains("\'") || new_val.contains("\"")){
					str=new_val.substring(1, new_val.length()-1);
					value[x] = str;
				}
				else {
					value[x] = new_val;
				}
			}
			
			String table_file = "data/"+tableName.toLowerCase()+".tbl";
			RandomAccessFile tableFile = new RandomAccessFile(table_file,"rw");
			
	      
			ArrayList<String> col = new ArrayList<String>();
			ArrayList<String> type = new ArrayList<String>();
			ArrayList<String> nullcheck = new ArrayList<String>();
			ArrayList<ArrayList<String>> arrayList = new ArrayList<ArrayList<String>>();
			try {
	  			arrayList = SelectData.getColumn(tableName);
				col = arrayList.get(0);                   				
				type = arrayList.get(1);								
				nullcheck = arrayList.get(2);
				Collections.reverse(nullcheck);
				Collections.reverse(col);
				Collections.reverse(type);
			} catch (Exception e) {
				e.printStackTrace();
			}
			String[] cons = (String[]) nullcheck.toArray(new String[nullcheck.size()]);
			String[] column_type = (String[]) type.toArray(new String[type.size()]); 
			
		   int[] serial_codes = new int[type.size()];
		   serial_codes = add_data(value,column_type,cons);
		   int pk = Integer.parseInt(value[0]);
		   
		if(CheckPK(pk,table_file) == 1) {										
		    if(serial_codes!=null) {
		        int payload = calculate_payload_columns(value,serial_codes);	
			    int req_space = payload+9;
			    File f = new File(table_file);
		        long fileLength = f.length();
		        int len = (int) fileLength;
		        
		        int no_of_pages = len/page_size;
		        int page_start = page_size*(no_of_pages-1);
		         int space = left_space(table_file, page_start);
		        int new_rowid = pk;
		        int newrow_space = payload+9; 
		        
		        tableFile.seek(page_start+1);
	        	int row_count = tableFile.readByte();
			   if(space>=newrow_space) {										// if space is available insert
				   tableFile.seek(page_start+1);
		           int inc_count = row_count+1;
		           tableFile.seek(page_start+1);
		           tableFile.writeByte(inc_count);
		           tableFile.seek(page_start+2);
		           int old_start;
		        	old_start = tableFile.readShort();
		        	
		        	
		        	int new_row_start = get_newrow_start(table_file,old_start,payload);
		        	tableFile.seek(page_start+2);
		        	tableFile.writeShort(new_row_start);
		        	tableFile.seek(page_start+(2*row_count)+4);
		        	tableFile.writeShort(new_row_start);     	
		        	tableFile.seek(new_row_start);	  
			   }
			   else {
		        	if(no_of_pages==1) {		        		 
		        		 int div = (int) Math.ceil(row_count / 2.0);
		        		
		        		 tableFile.seek(4+2*(div-1));
		        		 int new_start_con = tableFile.readShort();
		        		 tableFile.seek(4+2*(div-1));
		        		 
			        	 int end_copy = tableFile.readShort();
			        	 tableFile.seek(4+2*(row_count-1));
			        	 int start_copy = tableFile.readShort();
			        	 int diff = end_copy-start_copy;
			        	 
			        	 tableFile.seek(4+2*(div-1));
			        	 int[] add = new int[row_count-div+1];
			        	 for(int k=0;k<row_count-div+1;k++) {
			        		add[k] = tableFile.readShort();
			        	 }
			        	 
			        	 tableFile.seek(4+2*(div));
			        	 for(int x=div;x<row_count;x++) {
			        		 tableFile.writeShort(0x01);
			        	 }
			        	 tableFile.seek(1);
			        	 tableFile.writeByte(div);
			        	 tableFile.writeShort(new_start_con);
			        	 tableFile.seek(start_copy);
			        	 
			        	 byte[] out1 = new byte[diff];
			        	 tableFile.readFully(out1);
			        	 
			        	 
			        	 
		        		 tableFile.setLength(len+page_size);						
			        	 no_of_pages = no_of_pages+1;
			        	 int new_page_start = page_size*(no_of_pages-1);
			        	 tableFile.seek(new_page_start);
			        	 tableFile.writeByte(0x05);
			        	 tableFile.writeByte(0x01);
			        	 tableFile.writeShort(new_page_start+504);
			        	 tableFile.writeInt(3); 
			        	 tableFile.writeShort(new_page_start+504);
			        	 
			        	 tableFile.seek(new_page_start+504);
			 			 tableFile.writeInt(1);
			 			 tableFile.writeInt(div);				
	                     
			        	 
			        	 len = len+page_size;
			        	 tableFile.setLength(len+page_size);			
			        	 no_of_pages = no_of_pages+1;
			        	 new_page_start = page_size*(no_of_pages-1);
			        	 len = len+page_size;
			        	 tableFile.seek(new_page_start);
			        	 tableFile.writeByte(0x0D);	 
			        	 tableFile.writeByte(div);
			        	 
			        	 int[] diff_byte = new int[row_count-div];
			        	 for(int k=0;k<row_count-div;k++) {
			        		diff_byte[k] = add[k]-add[k+1];
			        	}
   	 
			        	 int off = len - diff;
			        	 tableFile.seek(new_page_start+1);
			        	 tableFile.writeByte(row_count-div);
			        	 tableFile.writeShort(off);
			        	 tableFile.seek(new_page_start+4);
			        	 int offset;
			        	for(int x=0;x<row_count-div;x++) {
			        		 offset = len - diff_byte[x];
			        		 tableFile.writeShort(offset);
			        		 len = len - diff_byte[x];
		       		 
			        	 }
		
			        	 tableFile.seek(off);
			        	 tableFile.write(out1);
			        	 int old_start = off;
			        	 int new_row_start = get_newrow_start(table_file,old_start,payload);
			        	 tableFile.seek(new_page_start+1);
			        	 int rows = tableFile.readByte();
			        	 tableFile.seek(new_page_start+1);
			        	 tableFile.writeByte(rows+1);
			        	 tableFile.writeShort(new_row_start);
			        	 tableFile.seek(new_page_start+4+2*rows);
			        	 tableFile.writeShort(new_row_start);
			        	 tableFile.seek(new_row_start);
			        	 
		        	}
		        	else if(no_of_pages+1>=3) {	        		
		        		int div = (int) Math.ceil(row_count / 2.0);
		        		 tableFile.setLength(len+page_size);
			        	 no_of_pages = no_of_pages+1;
			        	 int new_page_start = page_size*(no_of_pages-1);
			        	 len = len+page_size;
			        	 tableFile.seek(new_page_start);
			        	 tableFile.writeByte(0x0D);	 
			        	 tableFile.writeByte(div);
		        		 
		        		 tableFile.seek(page_start+4+2*(div-1));
		        		 int new_start_con = tableFile.readShort();
		        		 tableFile.seek(page_start+4+2*(div-1));
		        		 int end_copy = tableFile.readShort();
			        	 tableFile.seek(page_start+4+2*(row_count-1));
			        	 int start_copy = tableFile.readShort();
			        	 int diff = end_copy-start_copy;	        	 
			        	 tableFile.seek(page_start+4+2*(div-1));
			        	 int[] add = new int[row_count-div+1];
			        	 for(int k=0;k<row_count-div+1;k++) {
			        		add[k] = tableFile.readShort();
			        	 }
			        	 
			        	 tableFile.seek(page_start+4+2*(div));
			        	 for(int x=div;x<row_count;x++) {
			        		 tableFile.writeShort(0x01);
			        	 }
			        	 tableFile.seek(page_start+1);
			        	 tableFile.writeByte(div);
			        	 tableFile.writeShort(new_start_con);
			        	 tableFile.seek(start_copy);
			        	 
			        	 byte[] out1 = new byte[diff];
			        	 tableFile.readFully(out1);
			        	 
			        	 
			        	 
		        		 
			        	 tableFile.seek(new_page_start);
			        	 int[] diff_byte = new int[row_count-div];
			        	 for(int k=0;k<row_count-div;k++) {
			        		diff_byte[k] = add[k]-add[k+1];
			        	}        	 
			        	 int off = len - diff;
			        	 tableFile.seek(new_page_start+1);
			        	 tableFile.writeByte(row_count-div);
			        	 tableFile.writeShort(off);
       		        	 tableFile.seek(new_page_start+4);
			        	 int offset;
			        	 
			        	for(int x=0;x<row_count-div;x++) {
			        		 offset = len - diff_byte[x];
			        		 tableFile.writeShort(offset);
			        		 len = len - diff_byte[x];       		 
			        	 }
		                 tableFile.seek(513);
		                 int cells = tableFile.readByte();
		                 tableFile.seek(513);
		                 tableFile.writeByte(cells+1);
		                 int new_add = tableFile.readShort();
		                 tableFile.seek(514);
		                 tableFile.writeShort(new_add-8);
		                 tableFile.writeInt(no_of_pages);
		                 tableFile.seek(520+2*(no_of_pages-3));
		                 tableFile.writeShort(new_add-8);
		                 tableFile.seek(new_add-8);
		                 tableFile.writeInt(no_of_pages-1);
		                 tableFile.writeInt(new_rowid-row_count+div-1);
		                 
			        	 tableFile.seek(off);
			        	 tableFile.write(out1);
			        	 int old_start = off;
			        	 int new_row_start = get_newrow_start(table_file,old_start,payload);
			        	 tableFile.seek(new_page_start+1);
			        	 int rows = tableFile.readByte();
			        	 tableFile.seek(new_page_start+1);
			        	 tableFile.writeByte(rows+1);
			        	 tableFile.writeShort(new_row_start);
			        	 tableFile.seek(new_page_start+4+2*rows);
			        	 tableFile.writeShort(new_row_start);
			        	 tableFile.seek(new_row_start);		        	 
		        	}	        	
		        }
		  
			    tableFile.writeShort(payload);
		        tableFile.writeInt(Integer.parseInt(value[0]));
		        tableFile.writeByte(value.length-1);
		        for(int j=1;j<value.length;j++) {
		        	if(serial_codes[j] != 13 ) {
		        		tableFile.writeByte(serial_codes[j]);
		        	}
		        	else {
		        		tableFile.writeByte(12+value[j].length());
		        	}
		        }
		        for(int k=1;k<value.length;k++) {
		        	if(serial_codes[k]==4) {
		        		tableFile.writeByte(Integer.parseInt(value[k]));
		        	}
		        	if(serial_codes[k]==5) {
		        		tableFile.writeShort(Integer.parseInt(value[k]));
		        	}
		        	if(serial_codes[k]==6) {
		        		tableFile.writeInt(Integer.parseInt(value[k]));
		        	}
		        	if(serial_codes[k]==7) {
		        		tableFile.writeLong(Integer.parseInt(value[k]));
		        	}
		        	if(serial_codes[k]==8) {
		        		tableFile.writeFloat(Float.parseFloat(value[k]));
		        	}
		        	if(serial_codes[k]==9) {
		        		tableFile.writeDouble(Double.parseDouble(value[k]));
		        	}
		        	if(serial_codes[k]==10) {
		        		tableFile.writeBytes(value[k]);
		        	}
		        	if(serial_codes[k]==11) {
		        		tableFile.writeBytes(value[k]);
		        	}
		        	
		        	if(serial_codes[k]==13) {
		        		tableFile.writeBytes(value[k]);
		        	}
	        	
	        }
	        System.out.println("Insert sucessful");
	        }
		}
		else {
			System.out.println("Primary key must be unique. Cannot Insert given values");
		}
		   
			} catch (Exception e) {
				e.printStackTrace();
			}
	}
	
	
	private static int CheckPK(int row_id, String file_name) throws IOException {
		RandomAccessFile table_File = new RandomAccessFile(file_name,"rw");
		File f = new File(file_name);
        long fileLength = f.length();
        int len = (int) fileLength;
        int pages = len/page_size;
        for(int i=0;i<pages;i++) {
        	int page_start = (i)*512;
        	table_File.seek(page_start);
        	if(table_File.readByte() == 13) {
        		table_File.seek(page_start+1);
        		int rows = table_File.readByte();
        		table_File.seek(page_start+4);
        		int[] row_loc = new int[rows];
        		for(int k=0; k<rows;k++) {
        			row_loc[k] = table_File.readShort();
        		}
        		for(int x=0; x<rows;x++) {
        			 table_File.seek(row_loc[x]+2);
        			 int y = table_File.readInt();
        			 if(row_id == y) {
        				 return 0;
        			 }
        		}
        	}
        }
		return 1;
	}
	
	
	private static int get_newrow_start(String file_name,int old_start,int payload ) throws IOException {	
    	int new_start = old_start-payload-7;
    	return new_start;  	
	}
	
	
	private static int left_space(String file_name,int page_start) throws IOException {
		RandomAccessFile curr_file = new RandomAccessFile(file_name, "rw");
		curr_file.seek(page_start+1);
    	int no_of_rows = curr_file.readByte();
    	int additional_bytes = page_start+(2*no_of_rows)+4;
    	curr_file.seek(page_start+2);
    	int start = curr_file.readShort();
    	int space_left = start-additional_bytes;
    	curr_file.close();
    	return space_left;	
    	
	}
	
	
	private static int  calculate_payload_columns(String[] values, int[] serial_code) {
		int payload=0;
		
        for(int i=1;i<values.length;i++) {
            if(serial_code[i] == 13) {            
                payload = payload+values[i].length()+1;			
            }
                      
            if(serial_code[i] == 4) {
                payload = payload+1+1;							
            }
           
            if(serial_code[i] == 5) {
                payload = payload+2+1;							
            }
            int[] codes_3 = {6,8};  
            if(serial_code[i] == 6 || serial_code[i] == 8) {
                payload = payload+4+1;							
            }
            int[] codes_4 = {7,9};  
            if(serial_code[i] == 7 || serial_code[i] == 9) {
                payload = payload+8+1;							
            }
            int[] codes_5 = {0,1,2,3,12};  
            if(serial_code[i] == 0 || serial_code[i] == 1 || 
            		serial_code[i] == 2 || serial_code[i] == 3 ||
            		serial_code[i] == 12) {
                payload = payload+1;							
                
            }
            			
	        if(serial_code[i] == 11) {
	            payload = payload+10+1;							
	        }
	        if(serial_code[i] == 10) {
	            payload = payload+19+1;							
	        }
        }
        return payload;
		
	}
	
	
	private static int[] add_data(String[] values, String[] column_type, String[] cons ) {
		int[] serial_code = new int[column_type.length];
		for(int i=0;i<values.length;i++) {
			if(cons[i].equalsIgnoreCase("NO")) {
				if(values[i].equalsIgnoreCase("null") || values[i].isEmpty()) {
					System.out.println("given values not accepted");
					serial_code = null;
					break;
				}
				else {
					serial_code[i] = get_serialcode(column_type[i]);
					
				}
				
			}
			else {
				if(values[i].equalsIgnoreCase("null") || values[i].length()==0) {
					if(column_type[i].equalsIgnoreCase("TINYINT")) {
						serial_code[i] = 0;
					}
					else if(column_type[i].equalsIgnoreCase("SMALLINT")) {
						serial_code[i] = 1;
					}
					else if(column_type[i].equalsIgnoreCase("INT") || column_type[i].equalsIgnoreCase("REAL")) {
						serial_code[i] = 2;
					}
					else if(column_type[i].equalsIgnoreCase("DOUBLE") || column_type[i].equalsIgnoreCase("BIGINT")
							|| column_type[i].equalsIgnoreCase("DATETIME") ||
							column_type[i].equalsIgnoreCase("DATE")) {
						serial_code[i] = 3;
					}
					else if(column_type[i].equalsIgnoreCase("TEXT")) {
						serial_code[i] = 12;
					}
					
				}
				else {
					serial_code[i] = get_serialcode(column_type[i]);
					
				}
			}
				
			}
		return serial_code;
		}
	
	
	  private static int get_serialcode(String column_type) {
		  int serial_code=-1;
		  if(column_type.equalsIgnoreCase("INT")) {
				 serial_code = 6;					
			}
			else if(column_type.equalsIgnoreCase("SMALLINT")) {
				serial_code = 5;
			}
			else if(column_type.equalsIgnoreCase("TINYINT")) {
				serial_code = 4;
			}
			else if(column_type.equalsIgnoreCase("DOUBLE")) {
				serial_code = 9;
			}
			else if(column_type.equalsIgnoreCase("BIGINT")) {
				serial_code = 7;
			}
			else if(column_type.equalsIgnoreCase("REAL")) {
				serial_code = 8;
			}
			else if(column_type.equalsIgnoreCase("DATETIME")) {
				serial_code = 10;
			}
			else if(column_type.equalsIgnoreCase("DATE")) {
				serial_code = 11;
			}
			else if(column_type.equalsIgnoreCase("TEXT")) {
				serial_code = 13;
			}
		   return serial_code;
	  }
	}
