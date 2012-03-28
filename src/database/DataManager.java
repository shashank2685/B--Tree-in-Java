package database;

import java.util.Map;
import java.io.*;
import java.util.Hashtable;
import java.util.HashMap;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;

public class DataManager {

	private static Hashtable<String, DataFile> htTableStore = new Hashtable<String, DataFile>();

	public static DataFile getDataFile(String filename) {
		return htTableStore.get(filename);
	}

	public static void removeTable(String tableName)
	{
		htTableStore.remove(tableName);
	}
	
	public static Hashtable<String, DataFile> gethtTableStore() {
		return htTableStore;
	}
	
	public static DataFile restoreFile(String fileName) throws IOException {

		if (htTableStore.containsKey(fileName)) {
			throw new IllegalArgumentException();
		} else {
			File directory = new File("data/" + fileName + "/" + fileName);
			boolean exists = directory.exists();
			if (exists) {
				FileReader fr = new FileReader(directory);
				BufferedReader br = new BufferedReader(fr);

				int i = 0;
				String line = br.readLine();

				/*
				 * First read the number of columns form the file
				 */
				StringTokenizer st = new StringTokenizer(line, "|");
				int numCol = Integer.parseInt(st.nextToken());

				String name[] = new String[numCol];
				String len[] = new String[numCol];
				String values[] = new String[numCol];

				while (st.hasMoreTokens()) {

					name[i] = st.nextToken();
					len[i] = st.nextToken();
					i++;
				}

				/*
				 * Read the meta data form file. And create DataFile with those
				 * arguments.
				 */
				Map<String, Integer> colName_len = new HashMap<String, Integer>();
				for (i = 0; i < numCol; i++) {
					colName_len.put(name[i], Integer.parseInt(len[i]));
				}
				// Read Data
				DataFile obj = new DataFile(fileName, colName_len);
				while ((line = br.readLine()) != null) {

					StringTokenizer st1 = new StringTokenizer(line, "|");
					i = 0;
					int id = Integer.parseInt(st1.nextToken());
					while (st1.hasMoreTokens()) {

						values[i] = st1.nextToken();
						i++;
					}

					Map<String, String> values_len = new HashMap<String, String>();
					for (i = 0; i < numCol; i++) {
						values_len.put(name[i], values[i]);
					}
					obj.rows.put(id, values_len);
				}
				htTableStore.put(fileName, obj);
				fr.close();
				br.close();
				
				return obj;
				
			} else {
				throw new IllegalArgumentException();
			}

		}

	}

	public static DataFile createFile(String fileName,
			Map<String, Integer> descriptor) {

		/* first check table store if the data file exists */
		if (htTableStore.containsKey(fileName)) {
			String err = "ERROR: FileName already exists !\n";
			throw new IllegalArgumentException(err);
		}

		/* check if filename exists on data folder */
		/*File file = new File("data/" + fileName);
		boolean exists = file.exists();
		if (exists) {
			System.out.println("ERROR: FileName already exists !\n");
			throw new IllegalArgumentException();
		}*/
		
		/* check Column LENGTH , if greater than 25 throw error */
		Set<String> keys = descriptor.keySet();
		Iterator<String> it = keys.iterator();

		while (it.hasNext()) {

			String key = it.next();
			int value = descriptor.get(key);
			
			if(value>25)
				throw new IllegalArgumentException("DataManager:: Column Value Exceeds 25 characters !");

		}

		/* if its new datafile create a file */
		DataFile obj = new DataFile(fileName, descriptor);

		/* dump into hastable */
		htTableStore.put(fileName, obj);
		return obj;
	}

	/* exists the system closing all files and dumping datafile to disk */
	public static void exit() {

		/* dump all datafiles in table store */
		Enumeration<DataFile> e = htTableStore.elements();

		while (e.hasMoreElements()) {
			DataFile temp = (DataFile) e.nextElement();
			try {
				temp.dumpFile();
				temp.dumpAllIndex();
			} catch (IOException dumpFileException) {
				dumpFileException.printStackTrace();
			}
		}
		/* close any open files here */
		htTableStore.clear();		
	}

	public static String print(Map<String, String> record) {

		StringBuffer returnValue = new StringBuffer();

		Set<String> keys = record.keySet();
		Iterator<String> it = keys.iterator();

		while (it.hasNext()) {

			String key = it.next();
			String value = record.get(key);

			if (value != null && !value.isEmpty()) {
				returnValue.append('\t');
				returnValue.append(key);
				returnValue.append(": ");
				returnValue.append(value);
				returnValue.append("\n");
			}
		}
		return returnValue.toString();

	}

}
