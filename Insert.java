import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Map.Entry;

public class Insert {
	static int page_size = 512;
	
	public static void insert_columns(String[] array) {
		try {
			String column_file = "data/davisbase_columns.tbl";
			RandomAccessFile TableFile = new RandomAccessFile(column_file, "rw");
			int payload = calculate_payload_columns(array);
			int newrow_space = payload+9;                  // payload,row_address,columns,row_id                                     
			File f = new File(column_file);
	        long fileLength = f.length();
	        int len = (int) fileLength;
	        
	        int no_of_pages = len/page_size;
	        int page_start = page_size*(no_of_pages-1);
	        int space = left_space(column_file, page_start);
	        int new_rowid = get_newrow_id(column_file, no_of_pages);
	        TableFile.seek(page_start+1);
        	int row_count = TableFile.readByte();
        	
        	if(space>=newrow_space) {     
	        	int inc_count = row_count+1;
	        	TableFile.seek(page_start+1);
	        	TableFile.writeByte(inc_count);
	        	TableFile.seek(page_start+2);
	        	int old_start = TableFile.readShort();
	        	int new_row_start = get_newrow_start(column_file,old_start,payload);
	        	TableFile.seek(page_start+2);
	        	TableFile.writeShort(new_row_start);
	        	TableFile.seek(page_start+(2*row_count)+4);
	        	TableFile.writeShort(new_row_start);     	
	        	TableFile.seek(new_row_start);	        		
	        }
	        else {  
	        	if(no_of_pages==1) {	        		 
	        		 int div = (int) Math.ceil(row_count / 2.0);
	        		
	        		 TableFile.seek(4+2*(div-1));
	        		 int new_start_con = TableFile.readShort();
	        		 TableFile.seek(4+2*(div-1));
	        		 
		        	 int end_copy = TableFile.readShort();
		        	 TableFile.seek(4+2*(row_count-1));
		        	 int start_copy = TableFile.readShort();
		        	 int diff = end_copy-start_copy;
		        	 
		        	 TableFile.seek(4+2*(div-1));
		        	 int[] add = new int[row_count-div+1];
		        	 for(int k=0;k<row_count-div+1;k++) {
		        		add[k] = TableFile.readShort();
		        	 }
		        	 
		        	 TableFile.seek(4+2*(div));
		        	 for(int x=div;x<row_count;x++) {
		        		 TableFile.writeShort(0x01);
		        	 }
		        	 TableFile.seek(1);
		        	 TableFile.writeByte(div);
		        	 TableFile.writeShort(new_start_con);
		        	 TableFile.seek(start_copy);
		        	 
		        	 byte[] out1 = new byte[diff];
		        	 TableFile.readFully(out1);
		        	 
		        	 
		        	 
	        		 TableFile.setLength(len+page_size);
		        	 no_of_pages = no_of_pages+1;
		        	 int new_page_start = page_size*(no_of_pages-1);
		        	 TableFile.seek(new_page_start);
		        	 TableFile.writeByte(0x05);
		        	 TableFile.writeByte(0x01);
		        	 TableFile.writeShort(new_page_start+504);
		        	 TableFile.writeInt(3); 
		        	 TableFile.writeShort(new_page_start+504);
		        	 
		        	 TableFile.seek(new_page_start+504);
		 			 TableFile.writeInt(1);				// A pointer to the subtree < or = to the first key
		 			 TableFile.writeInt(div);				// The first key of the interior page/node
                     
		        	 
		        	 len = len+page_size;
		        	 TableFile.setLength(len+page_size);
		        	 no_of_pages = no_of_pages+1;
		        	 new_page_start = page_size*(no_of_pages-1);
		        	 len = len+page_size;
		        	 TableFile.seek(new_page_start);
		        	 TableFile.writeByte(0x0D);	 
		        	 TableFile.writeByte(div);
	        	
		        	 int[] diff_byte = new int[row_count-div];
		        	 for(int k=0;k<row_count-div;k++) {
		        		diff_byte[k] = add[k]-add[k+1];
		        	}
		        	
		        	 int off = len - diff;
		        	 TableFile.seek(new_page_start+1);
		        	 TableFile.writeByte(row_count-div);
		        	 TableFile.writeShort(off);
		        	 TableFile.seek(new_page_start+4);
		        	 int offset;
		        	for(int x=0;x<row_count-div;x++) {
		        		 offset = len - diff_byte[x];
		        		 TableFile.writeShort(offset);
		        		 len = len - diff_byte[x];
	       		 
		        	 }
	
		        	 TableFile.seek(off);
		        	 TableFile.write(out1);
		        	 int old_start = off;
		        	 int new_row_start = get_newrow_start(column_file,old_start,payload);
		        	 TableFile.seek(new_page_start+1);
		        	 int rows = TableFile.readByte();
		        	 TableFile.seek(new_page_start+1);
		        	 TableFile.writeByte(rows+1);
		        	 TableFile.writeShort(new_row_start);
		        	 TableFile.seek(new_page_start+4+2*rows);
		        	 TableFile.writeShort(new_row_start);
		        	 TableFile.seek(new_row_start);
		        	 
	        	}
	        	else if(no_of_pages+1>=3) {       		                  
	        		
	        		int div = (int) Math.ceil(row_count / 2.0);
	        		 TableFile.setLength(len+page_size);
		        	 no_of_pages = no_of_pages+1;
		        	 int new_page_start = page_size*(no_of_pages-1);
		        	 len = len+page_size;
		        	 TableFile.seek(new_page_start);
		        	 TableFile.writeByte(0x0D);	 
		        	 TableFile.writeByte(div);
	        		 
	        		 TableFile.seek(page_start+4+2*(div-1));
	        		 int new_start_con = TableFile.readShort();
	        		 TableFile.seek(page_start+4+2*(div-1));
	        		 int end_copy = TableFile.readShort();
		        	 TableFile.seek(page_start+4+2*(row_count-1));
		        	 int start_copy = TableFile.readShort();
		        	 int diff = end_copy-start_copy;
		        	
		        	 TableFile.seek(page_start+4+2*(div-1));
		        	 int[] add = new int[row_count-div+1];
		        	 for(int k=0;k<row_count-div+1;k++) {
		        		add[k] = TableFile.readShort();
		        	 }
		        	 
		        	 TableFile.seek(page_start+4+2*(div));
		        	 for(int x=div;x<row_count;x++) {
		        		 TableFile.writeShort(0x01);
		        	 }
		        	 TableFile.seek(page_start+1);
		        	 TableFile.writeByte(div);
		        	 TableFile.writeShort(new_start_con);
		        	 TableFile.seek(start_copy);
		        	 
		        	 byte[] out1 = new byte[diff];
		        	 TableFile.readFully(out1);
		        	 
		        	 
		        	 
	        		 
		        	 TableFile.seek(new_page_start);
		        	 int[] diff_byte = new int[row_count-div];
		        	 for(int k=0;k<row_count-div;k++) {
		        		diff_byte[k] = add[k]-add[k+1];
		        	}
		        	
		        	 int off = len - diff;
		        	 TableFile.seek(new_page_start+1);
		        	 TableFile.writeByte(row_count-div);
		        	 TableFile.writeShort(off);
		        	 TableFile.seek(new_page_start+4);
		        	 int offset;
		        	for(int x=0;x<row_count-div;x++) {
		        		 offset = len - diff_byte[x];
		        		 TableFile.writeShort(offset);
		        		 len = len - diff_byte[x];	       		 
		        	 }
	                 TableFile.seek(513);
	                 int cells = TableFile.readByte();
	                 TableFile.seek(513);
	                 TableFile.writeByte(cells+1);
	                 int new_add = TableFile.readShort();
	                 TableFile.seek(514);
	                 TableFile.writeShort(new_add-8);
	                 TableFile.writeInt(no_of_pages);
	                 TableFile.seek(520+2*(no_of_pages-3));
	                 TableFile.writeShort(new_add-8);
	                 TableFile.seek(new_add-8);
	                 TableFile.writeInt(no_of_pages-1);
	                 TableFile.writeInt(new_rowid-row_count+div-1);
	                 
		        	 TableFile.seek(off);
		        	 TableFile.write(out1);
		        	 int old_start = off;
		        	 int new_row_start = get_newrow_start(column_file,old_start,payload);
		        	 TableFile.seek(new_page_start+1);
		        	 int rows = TableFile.readByte();
		        	 TableFile.seek(new_page_start+1);
		        	 TableFile.writeByte(rows+1);
		        	 TableFile.writeShort(new_row_start);
		        	 TableFile.seek(new_page_start+4+2*rows);
		        	 TableFile.writeShort(new_row_start);
		        	 TableFile.seek(new_row_start);    	 
	        	} 	
	        }
		    String table_name = array[0];
		    String column_name = array[1];
		    String column_key = array[2];
		    String data_type = array[3];
		    String is_nullable = array[4];
		    int ord_pos = Integer.parseInt(array[5]);
	        TableFile.writeShort(payload);
	        TableFile.writeInt(new_rowid);
	        TableFile.writeByte(6);
	        TableFile.writeByte(12+table_name.length()); 
	        TableFile.writeByte(12+column_name.length()); 
	        TableFile.writeByte(12+column_key.length());
	        TableFile.writeByte(12+data_type.length());	        
	        TableFile.writeByte(5);
	        TableFile.writeByte(12+is_nullable.length());
	        TableFile.writeBytes(table_name);
	        TableFile.writeBytes(column_name);
	        TableFile.writeBytes(column_key);
	        TableFile.writeBytes(data_type);
	        TableFile.writeShort(ord_pos);
	        TableFile.writeBytes(is_nullable);
	        
	        
		
	        TableFile.close();
		}
		catch (Exception e) {
			System.out.println(e);
		}
	}
	public static void insert_into_tables(String table_name) {
		try {
			String table_file = "data/davisbase_tables.tbl";
			RandomAccessFile TableFile = new RandomAccessFile(table_file, "rw");
			int payload = calculate_payload_tables(table_name);
			int newrow_space = payload+9;                  // payload,row_address,columns,row_id                                    
			File f = new File(table_file);
	        long fileLength = f.length();
	        int len = (int) fileLength;
	        
	        int no_of_pages = len/page_size;
	        int page_start = page_size*(no_of_pages-1);
	        int space = left_space(table_file, page_start);
	        int new_rowid = get_newrow_id(table_file, no_of_pages);
	        if(space>=newrow_space) {
	        	
	        	TableFile.seek(page_start+1);
	        	int row_count = TableFile.readByte();
	        	int inc_count = row_count+1;
	        	TableFile.seek(page_start+1);
	        	TableFile.writeByte(inc_count);
	        	TableFile.seek(page_start+2);
	        	int old_start = TableFile.readShort();
	        	int new_row_start = get_newrow_start(table_file,old_start,payload);
	        	TableFile.seek(page_start+2);
	        	TableFile.writeShort(new_row_start);
	        	TableFile.seek(page_start+(2*row_count)+4);
	        	TableFile.writeShort(new_row_start);     	
	        	TableFile.seek(new_row_start);	        	
	        	
	        	
	        }
	        else {
	        	if(no_of_pages+1<3)
	        	   TableFile.setLength(len+page_size);
	        	   no_of_pages = no_of_pages+1;
	        	   int new_page_start = page_size*(no_of_pages-1);
	        	   TableFile.seek(new_page_start);
	        	   TableFile.writeByte(0x0D);
	        	   int row_count = 1;
	        	   TableFile.writeByte(row_count);
	        	   int new_row_start = (512*no_of_pages) - (payload+7);
	        	   TableFile.writeShort(new_row_start);
	        	   TableFile.seek(new_page_start+4);
	        	   TableFile.writeShort(new_row_start);
	        	   TableFile.seek(new_row_start);
	        }
		    
	        TableFile.writeShort(payload);
	        TableFile.writeInt(new_rowid);
	        TableFile.writeByte(1);
	        TableFile.writeByte(12+table_name.length()); 
	        TableFile.writeBytes(table_name);
	        TableFile.close();
		}
		
			
		
		catch (Exception e) {
			System.out.println("Unable to open " + "davisbase_tables");
		}
	}
	
	
	private static int get_newrow_id(String file_name, int no_of_pages) throws IOException {
		int current_rows =0;
		RandomAccessFile curr_file = new RandomAccessFile(file_name, "rw");
		for(int i=1;i<=no_of_pages;i++) {
			int page_start = page_size*(i-1);
			curr_file.seek(page_start);
			int page_type = curr_file.readByte();
			if(page_type == 13) {
				curr_file.seek(page_start+1);
				int row_count = curr_file.readByte();
				current_rows = current_rows+row_count;
			}
			
		}
		curr_file.close();
		int row_id = current_rows+1;
		return row_id;
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
	
	
	private static int get_newrow_start(String file_name,int old_start,int payload ) throws IOException {
    	int new_start = old_start-payload-7;
    	return new_start;
    	
	}
	
	/*
	 * Below method calculates payload
	 * for a new row of a davisbase_columns table
	 */
	private static int  calculate_payload_columns(String[] array) {
		int payload=0;
        for(int i=0;i<array.length;i++) {
            if(i!=5) {
                payload = payload+array[i].length()+1;			//Text length + 1 byte
            }
            else {
                payload = payload+2+1;							// ordinal position - short+1byte
            }
        }
        return payload;
		
	}
	
	
	private static int  calculate_payload_tables(String table_name) {
		int payload=0;  
        payload = payload+table_name.length()+1;			//Text length + 1 byte        
        return payload;
		
	}
}
			