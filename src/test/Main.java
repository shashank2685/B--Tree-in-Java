package test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.Iterator;

import database.*;

public class Main {
	
	public BufferedReader br;

	public Main() {

		InputStreamReader ir = new InputStreamReader(System.in);
		br = new BufferedReader(ir);
	}

	public static void main(String[] args) {

		int choice = 0;
		Main obj = new Main();
		while (true) {
			System.out.println("Hit Enter");
			try {
			obj.br.readLine();
			} catch (Exception e) {
				System.out.println("Received exception " + e.getClass());
				System.exit(0);
			}
			System.out.println("Enter \n1. Create Data File Object");
			System.out.println("2. Restore File");
			System.out.println("3. Exit");
			System.out.println("4. dumpfile");
			System.out.println("5. viewfile");
			System.out.println("6. dropfile");
			System.out.println("7. createIndex");
			System.out.println("8. dropIndex");
			System.out.println("9. restoreIndex");
			System.out.println("10. getRecord");
			System.out.println("11. insertRecord");
			System.out.println("12. dumpIndex");
			System.out.println("13. viewIndex");
			System.out.println("14. complete");
			System.out.println("15. Create DataFile from file");
			System.out.println("16. Insert Records from file");
			System.out.println("17. List all the data Files");
			Scanner in = new Scanner(System.in);
			choice = in.nextInt();

			switch (choice) {

			case 1:
				obj.createDataFile();
				break;
			case 2:
				obj.restoreFile();
				break;
			case 3:
				DataManager.exit();
				break;
			case 4:
				obj.dumpFile();
				break;
			case 5:
				obj.viewFile();
				break;
			case 6:
				obj.dropFile();
				break;
			case 11:
				obj.insertRecord();
				break;
			case 14:
				System.exit(0);
			case 15:
				obj.createDataFileFromFile();
				break;
			case 16:
				obj.insertRecordFromFile();
				break;
			case 17:
				obj.listAllDataFiles();
				break;
			default:
				System.out.println("Illegal value");
			}
		}
	}
	
	private void insertRecordFromFile() {
	
		System.out.println("Enter Datafile filename : ");
		try {
			String filename = br.readLine();
			DataFile obj = DataManager.getDataFile(filename);
			if (obj != null) {
				System.out.println("Enter file name from which read the input");
				String input = br.readLine();
				FileReader fr = new FileReader(input);
				Scanner sc = new Scanner(fr);
				Map<String, Integer> col = obj.getColumnMap();
				while(sc.hasNext()){
					
					HashMap<String, String> map = new HashMap<String, String>();
					int size = col.size();
					while(size > 0) {
						String colName = sc.next();
						String line = sc.next();
						map.put(colName, line);
						size--;
					}
					int recid = obj.insertRecord(map);
					System.out.println("Inserted record " + recid);
				}

			} else {
				System.out.println("No DataFile object by the name " + filename);
			}
		} catch (Exception e) {
			System.out.println("Caught exception: " + e.getClass());
			e.printStackTrace();
		}
		
	}
	
	private void listAllDataFiles() {
	
		Hashtable<String, DataFile> ht = DataManager.gethtTableStore();
		Set<String> st = ht.keySet();
		Iterator<String> it = st.iterator();
		
		while(it.hasNext()) {
			String dataFileName = it.next();
			DataFile obj = ht.get(dataFileName);
			System.out.println(dataFileName + ":");
			Map<String, Integer> col = obj.getColumnMap();
			Set<String> colKeySet = col.keySet();
			Iterator<String> colit = colKeySet.iterator();
			while(colit.hasNext()) {
				String colName = colit.next();
				System.out.println("\t" + colName + ":" + col.get(colName));
			}
		}
		
	}
	
	private void createDataFileFromFile() {

		System.out.println("Enter the FileName form where read the input");
		try {
			String filename = br.readLine();
			FileReader fr = new FileReader(filename);
			BufferedReader filebuffer = new BufferedReader(fr);

			
			try {
				while(true) {
					String datafilename = filebuffer.readLine();
					if (null == datafilename)
						break;
					int noCols = Integer.parseInt(filebuffer.readLine());
					HashMap<String, Integer> map= new HashMap<String, Integer>();
					while(noCols > 0) {
						String colName = filebuffer.readLine();
						int colSize = Integer.parseInt(filebuffer.readLine());
						map.put(new String(colName), new Integer(colSize));
						noCols--;
					}
					DataManager.createFile(datafilename, map);
				}
			}catch(IOException e) {
				/* end of file Do nothing*/
			}
		} catch (Exception e) {
			System.out.println("Received Exception " + e.getClass());
		}
	}


	private void insertRecord() {
		System.out.println("Enter the filename: ");
		Scanner in = new Scanner(System.in);
		String filename = in.next();
		HashMap<String, String> record = new HashMap<String, String>();

		try {
			DataFile obj = DataManager.getDataFile(filename);
			Map<String, Integer> column = obj.getColumnMap();
			Set<String> columnName = column.keySet();
			Iterator<String> it = columnName.iterator();
			while (it.hasNext()) {
				String colname = it.next();
				System.out.println("Enter value for column " + colname);
				String colvalue = in.next();
				record.put(colname, colvalue);
			}
			obj.insertRecord(record);
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}

	}

	private void dropFile() {
		System.out.println("Enter the filename: ");
		Scanner in = new Scanner(System.in);
		String filename = in.next();
		try {
			DataFile obj = DataManager.getDataFile(filename);
			obj.dropFile();
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}

	private void viewFile() {
		System.out.println("Enter the filename: ");
		Scanner in = new Scanner(System.in);
		String filename = in.next();
		try {
			DataFile obj = DataManager.getDataFile(filename);
			System.out.println(obj.viewFile());
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}

	}

	private void dumpFile() {
		System.out.println("Enter the filename: ");
		Scanner in = new Scanner(System.in);
		String filename = in.next();
		try {
			DataFile obj = DataManager.getDataFile(filename);
			obj.dumpFile();
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}

	private void restoreFile() {
		Scanner in = new Scanner(System.in);
		try {
			System.out.println("Enter file name to be restored");
			String filename = in.next();
			DataManager.restoreFile(filename);
		} catch (IllegalArgumentException e) {
			System.out.println("Caught IllegalArgumentException");
		} catch (IOException e) {
			System.out.println("Caught IOException Error");
		} catch (Exception e) {
			System.out.println("Caught Exception");
		}
	}

	private void createDataFile() {

		int numColumns = 0;

		Scanner in = new Scanner(System.in);
		System.out.println("Enter the name of DataFile");
		String name = in.next();
		System.out.println("Enter number of columns in the datafile");
		numColumns = in.nextInt();
		HashMap<String, Integer> datafile = new HashMap<String, Integer>();
		int i = 0;

		while (i < numColumns) {
			System.out.println("Enter column " + (i + 1));
			String column = in.next();
			System.out.println("Enter column " + (i + 1) + "max length");
			int length = in.nextInt();

			datafile.put(column, new Integer(length));
			i++;
		}

		try {
			DataManager.createFile(name, datafile);
		} catch(Exception e) {
			System.out.println("Caught Exception : " + e.getClass());
		}
	}
}
