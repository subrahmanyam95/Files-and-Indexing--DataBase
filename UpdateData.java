import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;


public class UpdateData {
	static int page_size = 512;

	public static void updatemethod(String userCommand) {
		String[] token = userCommand.split(" ");
		String tableName = token[1];
		String col_key = null;
		String value;
		String value1 = null;
		String operator = "";
		int num = -1, num1 = -1;
		String type;
		int rowaddress = -1;
		String[] strArr = userCommand.split("[ =\\>\\<]");
		value = strArr[4];
		if (value.contains("\'") || value.contains("\"")) {
			value = value.substring(1, value.length() - 1);
		}
		value1 = strArr[strArr.length - 1];
		if (userCommand.contains("=")) {
			operator = "=";
			col_key = strArr[6];
		} else if (userCommand.contains(">")) {
			operator = ">";
			col_key = strArr[6];
		} else if (userCommand.contains("<")) {
			operator = "<";
			col_key = strArr[6];

		} else {
			operator = "none";
		}

		ArrayList<ArrayList<String>> arrayList = new ArrayList<ArrayList<String>>();
		ArrayList<String> col = new ArrayList<String>();
		ArrayList<String> typ = new ArrayList<String>();
		ArrayList<String> nullcheck = new ArrayList<String>();
		arrayList = SelectData.getColumn(tableName);
		col = arrayList.get(0);
		Collections.reverse(col);
		typ = arrayList.get(1);
		Collections.reverse(typ);
		nullcheck = arrayList.get(2);
		Collections.reverse(nullcheck);
		for (int i = 0; i < col.size(); i++) {
			if (col.get(i).equalsIgnoreCase(col_key)) {
				num = i;
			}
		}
		for (int i = 0; i < col.size(); i++) {
			if (col.get(i).equalsIgnoreCase(strArr[3])) {
				num1 = i;
			}
		}
		try {
			type = "";
			File fileTemp = new File("data/" + tableName.toLowerCase() + ".tbl");
			if (fileTemp.exists()) {
				RandomAccessFile tableFile = new RandomAccessFile("data/" + tableName.toLowerCase() + ".tbl", "rw");
				File f1 = new File("data/" + tableName.toLowerCase() + ".tbl");
				long fileLength = f1.length();
				int len1 = (int) fileLength;
				int no_of_pages = len1 / page_size;
				int page_start = 0;
				ArrayList<ArrayList<String>> outList = new ArrayList<ArrayList<String>>();
				while (no_of_pages != 0) {
					byte z1 = tableFile.readByte(); 
					if (z1 == 13) {
						byte z2 = tableFile.readByte(); 
						int z3 = tableFile.readShort(); 
														
						int[] z4 = new int[z2];
						LinkedList<Integer> l1 = new LinkedList<Integer>();
						for (int i = 0; i < z2; i++) {
							z4[i] = tableFile.readShort(); 
						}

						for (int i = 0; i < z2; i++) {
							if (z4[i] != 1) {
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
								if (c[i] == 0) {
									if (a[i] == 0 || a[i] == 1 || a[i] == 2 || a[i] == 3) {
									
										insideList.add("NULL");
									}
									if (a[i] == 4) {
										insideList.add(Integer.toString((int) tableFile.readByte()));
									}
									if (a[i] == 5) {
										insideList.add(Integer.toString(tableFile.readUnsignedShort()));
									}
									if (a[i] == 6) {
										insideList.add(Integer.toString(tableFile.readInt()));
									}
									if (a[i] == 7) {
										insideList.add(Long.toString(tableFile.readLong())); // Bigint
									}
									if (a[i] == 8) {
										insideList.add(Float.toString(tableFile.readFloat())); // Real
									}
									if (a[i] == 9) {
										insideList.add(Double.toString(tableFile.readDouble()));
									}
									if (a[i] == 10) {
										String str = "";
										for (int l = 0; l < 19; l++)
											str += (char) tableFile.readByte();
										insideList.add(str);
									}
									if (a[i] == 11) {

										
										String str = "";
										for (int l = 0; l < 10; l++)
											str += (char) tableFile.readByte();
										insideList.add(str);
									}

								} else {
									int len = a[i];
									String str = "";
									for (int l = 0; l < len; l++)
										str += (char) tableFile.readByte();
									insideList.add(str);
								}
							}

							if (insideList.get(num).equals(value1)) {
								rowaddress = l1.get(j);
								System.out.println();
								rowaddress = calculate_address_write(insideList, rowaddress, num1, typ) + x3;
								String message = writeprocedure((rowaddress), tableFile, typ, num1, value);
								System.out.println(message);
							}
							outList.add(insideList);

						}
						page_start = page_start + 512;
						tableFile.seek(page_start);
						no_of_pages--;
					} else {
						page_start = page_start + 512;
						tableFile.seek(page_start);
						no_of_pages--;
					}
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	
	private static String writeprocedure(int rowaddress, RandomAccessFile tableFile, ArrayList<String> typ, int num,
			String value) {
		try {
			tableFile.seek(rowaddress);
			if (typ.get(num).equalsIgnoreCase("int")) {
				tableFile.writeInt(Integer.parseInt(value));

			}
			if (typ.get(num).equalsIgnoreCase("smallint")) {
				tableFile.writeShort(Integer.parseInt(value));

			}
			if (typ.get(num).equalsIgnoreCase("double")) {
				tableFile.writeDouble(Double.parseDouble(value));
			}
			if (typ.get(num).equalsIgnoreCase("long") || typ.get(num).equalsIgnoreCase("bigint")) {
				tableFile.writeLong(Long.parseLong(value));
			}
			if (typ.get(num).equalsIgnoreCase("tinyint")) {
				System.out.println("Tiny type" + Integer.parseInt(value));
			}
			if (typ.get(num).equalsIgnoreCase("real")) {
				tableFile.writeFloat(Float.parseFloat(value));
			}
			if (typ.get(num).equalsIgnoreCase("text")) {
				tableFile.writeBytes(value);
			}
			if (typ.get(num).equalsIgnoreCase("date")) {
				tableFile.writeBytes(value);
			}
			if (typ.get(num).equalsIgnoreCase("datetime")) {
				tableFile.writeBytes(value);
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
		return "Updated one row";

	}

	
	private static int calculate_address_write(ArrayList<String> insideList, int rowaddress, int num1,
			ArrayList<String> typ) {
		int startplaceafter = 0;
		for (int i = 0; i < insideList.size(); i++) {
			if (i < num1) {
				if (typ.get(i).equalsIgnoreCase("int") || typ.get(i).equalsIgnoreCase("real")) {
					startplaceafter = startplaceafter + 4;
				}
				if (typ.get(i).equalsIgnoreCase("double")) {
					startplaceafter = startplaceafter + 8;
				}
				if (typ.get(i).equalsIgnoreCase("long") || typ.get(i).equalsIgnoreCase("bigint")
						|| typ.get(i).equalsIgnoreCase("date") || typ.get(i).equalsIgnoreCase("datetime")) {
					startplaceafter = startplaceafter + 8;
				}
				if (typ.get(i).equalsIgnoreCase("short")) {
					startplaceafter = startplaceafter + 2;
				}
				if (typ.get(i).equalsIgnoreCase("text")) {
					startplaceafter = startplaceafter + insideList.get(i).length();
				}
				if (typ.get(i).equalsIgnoreCase("tinyint")) {
					startplaceafter = startplaceafter + 1;
				}
				if (typ.get(i).equalsIgnoreCase("smallint")) {
					startplaceafter = startplaceafter + 2;
				}
			}
		}

		rowaddress = rowaddress + startplaceafter + 3;

		return rowaddress;
	}

}
