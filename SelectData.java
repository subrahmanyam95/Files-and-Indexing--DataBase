
import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;

public class SelectData {
	static int page_size=512;
	public static void selectall(String userCommand) {
		String[] token = userCommand.split(" ");
		String param=null;
		String[] paramvalues=null;
		if(token[1].contains("*")){
			param="all";
		}
		else{
			paramvalues=token[1].split(",");
		}
		String tableName = token[3];
		String col_key = null;
		String value;
		String operator = "";
		int num = -1;
		String type;
		String coltype;
		String[] strArr = userCommand.split("[ =\\>\\<]");
		ArrayList<ArrayList<String>> arrayList = new ArrayList<ArrayList<String>>();
		ArrayList<String> col = new ArrayList<String>();
		ArrayList<String> typ = new ArrayList<String>();
		ArrayList<String> nullcheck = new ArrayList<String>();
		ArrayList printlist=new ArrayList();
		try {
			
			value = strArr[strArr.length - 1];
			if (userCommand.contains("=")) {
				operator = "=";
				col_key = strArr[5];
			}
			else if (userCommand.contains(">")) {
				operator = ">";
				col_key = strArr[5];
			}
			else if (userCommand.contains("<")) {
				operator = "<";
				col_key = strArr[5];

			}
			else
			{
				operator="none";
			}
			arrayList = getColumn(tableName);
			col = arrayList.get(0);
			Collections.reverse(col);
			typ = arrayList.get(1);
			Collections.reverse(typ);
			nullcheck = arrayList.get(2);
			Collections.reverse(nullcheck);
				type = "";
		        File fileTemp = new File("data/" + tableName.toLowerCase()+ ".tbl");
         if (fileTemp.exists()){
				RandomAccessFile tableFile = new RandomAccessFile("data/" + tableName.toLowerCase() + ".tbl", "rw");
				File f1 = new File("data/" + tableName.toLowerCase() + ".tbl");
		        long fileLength = f1.length();
		        int len1 = (int) fileLength;
				int no_of_pages = len1/page_size;
				int page_start=0;
				ArrayList<ArrayList<String>> outList = new ArrayList<ArrayList<String>>();
				
			while(no_of_pages!=0){
				byte z1 = tableFile.readByte(); 
				if(z1==13){
				byte z2 = tableFile.readByte(); 
				int z3 = tableFile.readShort(); 
				int[] z4 = new int[z2];
				LinkedList<Integer> l1=new LinkedList<Integer>();
				for (int i = 0; i < z2; i++) {
					z4[i] = tableFile.readShort(); 
				}
				
				for(int i=0;i<z2;i++){
					if(z4[i]!=1){
					l1.add(z4[i]);
					}
				}
				
				for (int j = 0; j < l1.size(); j++) {
					ArrayList<String> insideList = new ArrayList<String>();
					tableFile.seek(l1.get(j));
						int x1 = tableFile.readShort(); 
						int x2 = tableFile.readInt(); 
						insideList.add(Integer.toString(x2));
						int x3 = tableFile.readByte(); 
						int[] a = new int[x3];
						int[] c = new int[x3];
						for (int i = 0; i < x3; i++) {
							int b = tableFile.readByte();
							if (b > 12) {
								a[i] = b - 12;
								c[i] = 1;
							} else {
								a[i] = b;
								c[i] = 0;
							}
						}

						for (int i = 0; i < x3; i++) {
							
							if(c[i]==0){
								if(a[i]==0 || a[i]==1 || a[i]==2 || a[i]==3)
								{
									
									 insideList.add("NULL");
									
								}
								if(a[i]==4)
								{
									
									insideList.add(Integer.toString((int)tableFile.readByte()));
								}
								if(a[i]==5)
								{
									insideList.add(Integer.toString(tableFile.readUnsignedShort()));
								}
								if(a[i]==6)
								{
									insideList.add(Integer.toString(tableFile.readInt()));
								}
								if(a[i]==7)
								{
									insideList.add(Long.toString(tableFile.readLong())); //Bigint
								}
								if(a[i]==8)
								{
									insideList.add(Float.toString(tableFile.readFloat())); //Real
								}
								if(a[i]==9)
								{
									insideList.add(Double.toString(tableFile.readDouble()));
								}
								if(a[i]==10)
								{
									String str = "";
									for (int l = 0; l < 19; l++)
										str += (char) tableFile.readByte();
									insideList.add(str);
								}
								if(a[i]==11)
								{
									
									
									String str = "";
									for (int l = 0; l < 10; l++)
										str += (char) tableFile.readByte();
									insideList.add(str);		
								}
					        	
							}
							else{
								int len = a[i];
								String str = "";
								for (int l = 0; l < len; l++)
									str += (char) tableFile.readByte();
								insideList.add(str);
								}
							}
				  outList.add(insideList);
				}
				page_start =page_start+512;
				tableFile.seek(page_start);
				no_of_pages--;	
			   }
				else{
					page_start =page_start+512;
					tableFile.seek(page_start);
					no_of_pages--;	
				}
			}
				
				int flag=0;
				ArrayList<Integer> intlist = new ArrayList<Integer>();
				for(int k=0;k<outList.size();k++)
				{
					flag=0;
					ArrayList l = outList.get(k);
					// **** equals comparison ** //
					for (int i =0; i<col.size(); i++) {
						if (col.get(i).equalsIgnoreCase(col_key)) {
							num = i;
						}
					}
					
					if(operator.equals("=") || operator.equals("<") || operator.equals(">")){
						if(operator.equals("="))
						{
						String str="";
						if(value.contains("\'") || value.contains("\"")){
							str=value.substring(1, value.length()-1);
						}
						else{
							str=value;
						}
						
						int p=l.get(num).toString().toLowerCase().compareTo(str.toLowerCase());
						if(p==0)
						{
							intlist.add(k);
						}
					 }
					
					 if(operator.equals(">"))
					 {
						coltype = typ.get(num);
						int g = -1;
						int v = -1;
						if(coltype.equalsIgnoreCase("int") || coltype.equalsIgnoreCase("TINYINT")){
							v= Integer.parseInt(value);
							String a=(String)l.get(num);
							if(a.equalsIgnoreCase("null")){
								
							}
							else{
							g=Integer.parseInt((String) l.get(num));
							}
						}
						else if(coltype.equalsIgnoreCase("short")|| coltype.equalsIgnoreCase("smallint")){
							v= Short.parseShort(value);
							String a=(String)l.get(num);
							if(a.equalsIgnoreCase("null")){
								
							}
							else{
							g=Short.parseShort((String) l.get(num));
							}
							
						}
						else if(coltype.equalsIgnoreCase("long") || coltype.equalsIgnoreCase("BIGINT") ||  coltype.equalsIgnoreCase("DATETIME")
								|| coltype.equalsIgnoreCase("DATE")){
							long f= Long.parseLong(value);
							String a=(String)l.get(num);
							if(a.equalsIgnoreCase("null")){
								
							}
							else{
							long m = Long.parseLong((String)l.get(num));
							if(m>f){
								intlist.add(k);
							}
							flag=1;
							}
						}
						else if(coltype.equalsIgnoreCase("float") || coltype.equalsIgnoreCase("Real")){
							float f= Float.parseFloat(value);
							String a=(String)l.get(num);
							if(a.equalsIgnoreCase("null")){

							}
							else{
							float m = Float.parseFloat((String) l.get(num));
							if(m>f){
								intlist.add(k);
							}
							flag=1;
							}
						}
						else if(coltype.equalsIgnoreCase("double")){
							double f= Double.parseDouble(value);
							String a=(String)l.get(num);
							if(a.equalsIgnoreCase("null")){
								
							}
							else{
							double m = Double.parseDouble((String) l.get(num));
							if(m>f){
								intlist.add(k);
							}
							flag=1;
							}
						}
						else if(coltype.equalsIgnoreCase("byte") ){
							byte f= Byte.parseByte(value);
							String a=(String)l.get(num);
							if(a.equalsIgnoreCase("null")){
								
							}
							else{
							byte m = Byte.parseByte((String) l.get(num));
							if(m>f){
								intlist.add(k);
							}
							flag=1;
							}
						}
						else{
							String str="";
							if(value.contains("\'") || value.contains("\"")){
								str=value.substring(1, value.length()-1);
							}
							else{
								str=value;
							}
							int p=l.get(num).toString().toLowerCase().compareTo(str.toLowerCase());
							if(p>0)
							{
								intlist.add(k);
							}
							flag=1;
						}

						if(g>v && flag==0)
						{
							intlist.add(k);
						}
					}
					
					if(operator.equals("<"))
					{
						coltype = typ.get(num);
						int g = -1;
						int v = -1;
						if(coltype.equalsIgnoreCase("int") || coltype.equalsIgnoreCase("TINYINT")){
							v= Integer.parseInt(value);
							String a=(String)l.get(num);
							if(a.equalsIgnoreCase("null")){
								
							}
							else{
							g=Integer.parseInt((String) l.get(num));
							}
						}
						else if(coltype.equalsIgnoreCase("short")|| coltype.equalsIgnoreCase("smallint")){
							v= Short.parseShort(value);
							String a=(String)l.get(num);
							if(a.equalsIgnoreCase("null")){
								
							}
							else{
							g=Short.parseShort((String) l.get(num));
							}
						}
						else if(coltype.equalsIgnoreCase("long") || coltype.equalsIgnoreCase("BIGINT")){
							long f= Long.parseLong(value);
							String a=(String)l.get(num);
							if(a.equalsIgnoreCase("null")){
								
							}
							else{
							long m = Long.parseLong((String) l.get(num));
							
							if(m<f){
								intlist.add(k);
							}
							//System.out.println("in long");
							flag=1;
							}
						}
						else if(coltype.equalsIgnoreCase("float") || coltype.equalsIgnoreCase("Real")){
							float f= Float.parseFloat(value);
							String a=(String)l.get(num);
							if(a.equalsIgnoreCase("null")){
								// intlist.add(k);
							}
							else{
							float m = Float.parseFloat((String) l.get(num));
							if(m<f){
								intlist.add(k);
							}
							flag=1;
							}
						}
						else if(coltype.equalsIgnoreCase("double")){
							double f= Double.parseDouble(value);
							String a=(String)l.get(num);
							if(a.equalsIgnoreCase("null")){
								// intlist.add(k);
							}
							else{
							double m = Double.parseDouble((String) l.get(num));
							if(m<f){
								intlist.add(k);
							}
							flag=1;
							}
						}
						else if(coltype.equalsIgnoreCase("byte")){
							byte f= Byte.parseByte(value);
							String a=(String)l.get(num);
							if(a.equalsIgnoreCase("null")){
								// intlist.add(k);
							}
							else{
							byte m = Byte.parseByte((String) l.get(num));
							if(m<f){
								intlist.add(k);
							}
							flag=1;
							}
						}
						else{
							String str="";
							if(value.contains("\'") || value.contains("\"")){
								str=value.substring(1, value.length()-1);
							}
							else{
								str=value;
							}
							int p=l.get(num).toString().toLowerCase().compareTo(str.toLowerCase());
							if(p<0)
							{
								intlist.add(k);
							}
							flag=1;
						}

						if(g<v && flag==0)
						{
							intlist.add(k);
						}
					  }
				
				  }
				  if(operator=="none"){
						intlist.add(k);	
						num=1;
			      }
				}
				
				if(outList.isEmpty()){
					num=1;
				}
				
				if(num==-1)
				{
					System.out.println("The column name doesnot exist");
				}
				else{
					lineDisplay();
					for (int i =0; i<col.size(); i++) {
						
						if(param!="all"){
						for(int j=0;j<paramvalues.length;j++){
							if(col.get(i).equalsIgnoreCase(paramvalues[j])){
								printlist.add(i);
								System.out.print(col.get(i) + "     |    ");
							}
						  }
						}
						else{
								System.out.print(col.get(i) + "     |    ");			 
						   }
					}
					System.out.println();
					lineDisplay();
					for(int t1=0; t1<intlist.size();t1++)
					{
						//	Display header			
						ArrayList result = outList.get(intlist.get(t1));

						for(int h=0;h<result.size();h++)
						{
							if(param=="all"){
							System.out.print(result.get(h)+"    |     ");
							}
							else{
									if(printlist.contains(h)){
										System.out.print(result.get(h)+"    |     ");
									}
								
							}
						}
						System.out.println();
					  }
				  }
			}
         
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static ArrayList<ArrayList<String>> getColumn(String tableName) {

		ArrayList<ArrayList<String>> arrayList = new ArrayList<ArrayList<String>>();
		ArrayList<String> col = new ArrayList<String>();
		ArrayList<String> typ = new ArrayList<String>();
		ArrayList<String> nullcheck = new ArrayList<String>();

		try {		
			RandomAccessFile columnsTableFile = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
		    long fileLength = columnsTableFile.length();
		    int len1 = (int) fileLength;
			int no_of_pages = len1/page_size;
			 int page_start = page_size*(no_of_pages-1);
			
	   while(no_of_pages!=0){
		    columnsTableFile.seek(page_start);
			byte z1 = columnsTableFile.readByte(); 
			if(z1==13){
			int z2 = columnsTableFile.readByte(); 
			int z3 = columnsTableFile.readShort(); 
			columnsTableFile.seek(z3);
			int item = -18;

			do {
				String str = "", str2 = "", str3 = "";
				String str1;
				String columnName = "";
				String type = "";
				String type2 = "", type3 = "";
				int flag1 = 0;
				int flag = 0;

				if (columnsTableFile.getFilePointer() < columnsTableFile.length()) {
					int x1 = columnsTableFile.readShort(); 
					int x2 = columnsTableFile.readInt(); 
					int x3 = columnsTableFile.readByte(); 
															
															
					if (item == -18) {
						item = z2;
					}
					int[] a = new int[x3 + 1]; 
					int[] c = new int[x3 + 1]; 
					String[] store = new String[x3 - 2];
					for (int i = 1; i <= x3; i++) {
						int b = columnsTableFile.readByte();
						if (b > 12) {
							a[i] = b - 12; 
							c[i] = 1;
						} else {
							a[i] = b;
							c[i] = 0;
						}
					}

					for (int k = 1; k < 3; k++) {
						str1 = "";
						columnName = "";

						for (int j = 0; j < a[k]; j++)
							str1 += (char) columnsTableFile.readByte();

						if (tableName.equalsIgnoreCase(str1)) {
							
							flag1 = 1;
						}
						if (k == 2 && flag1 == 1) {
							
							columnName = str1;
						}
					}

					for (int r = 3; r <= x3; r++) { 

						if (c[r] == 1) {
							for (int j = 0; j < a[r]; j++) {
								str += (char) columnsTableFile.readByte();
							}

							if (flag1 == 1) {
								store[r - 3] = str;
								str = "";
							}

						}

						else if (c[r] == 0) {
							
							if (a[r] == 5) {
								int num = columnsTableFile.readShort();
							}

							if (a[r] == 6 || a[r] == 8) {
								int num = columnsTableFile.readInt();
							}

							if (a[r] == 9 || a[r] == 7) {
								double num = columnsTableFile.readDouble();
							}

							if (a[r] == 10 || a[r] == 11) {
								for (int j = 0; j < a[r]; j++) {
									str += (char) columnsTableFile.readByte();
								}
							}
						}
					}

					if (flag1 == 1) {
						col.add(columnName);
						typ.add(store[1]);
						nullcheck.add(store[3]);
					}
				}
				item--;
			} while (item > 0);		
	      page_start =page_start-512;
		  no_of_pages--;
			
	      }
			else{
				page_start =page_start-512;
				no_of_pages--;
			}
			
	   }
		} catch (Exception e) {
			e.printStackTrace();
		}
		arrayList.add(col);
		arrayList.add(typ);
		arrayList.add(nullcheck);
		return arrayList;
	}
	
	private static void lineDisplay() {
		for (int i = 0; i < 10; i++)
			System.out.print("-------------");
		System.out.println();

	}
}
