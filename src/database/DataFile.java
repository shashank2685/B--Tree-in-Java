package database;

import java.util.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;

public class DataFile {

	private Map<String, Integer> colName;
	private String fileName;
	public HashMap<Integer, Map<String, String>> rows;
	private Hashtable<String, Index> indexList;
	public Integer id; 

	public Map<String, Integer> getColumnMap() {
		return this.colName;
	}

	public DataFile(String fileName, Map<String, Integer> colName) {

		this.colName = colName;
		this.fileName = fileName;
		this.rows = new HashMap<Integer, Map<String, String>>();
		this.indexList = new Hashtable<String, Index>();
		this.id = 0;
	}

	public String getFileName() {
		return this.fileName;
	}

	public Index createIndex(String indexName, String column) {

		/*
		 * Check whether the index with indexName already exists. If exists
		 * throw IllegalAtfumentException
		 */
		if (this.indexList.containsKey(indexName)) {
			String msg = "IndexName " + indexName + " Already exists in Memory";
			throw new IllegalArgumentException(msg);
		}

		if (!this.colName.containsKey(column)) {
			String msg = "Column " + column + " Doesnot exists in this table";
			throw new IllegalArgumentException(msg);
		}

		/*
		 * For every row in the table call insert column to the index
		 */
		Index currentIndex = new Index(column, this,indexName);
		this.indexList.put(indexName, currentIndex);

		Iterator<Integer> it = this.rows.keySet().iterator();
		int i = 0;

		while (it.hasNext()) {
			Integer id = it.next();
			Map<String, String> row = this.rows.get(id);
			String value = row.get(column);
			if (null == value) {
				i++;
			} else {
				currentIndex.insertValue(id, value);
			}
		}

		return currentIndex;
	}

	public void dumpFile() throws IOException {

		File file = new File("data/" + fileName);
		if (! file.exists() ) {
			if ( ! file.mkdirs() ) {
				System.out.println("Unable to Dump the datafile " + fileName);
				System.out.println("Dump is Unscsseessfull");
				return;
			}
		}
		file = new File("data/" + fileName + "/" + fileName);
		FileOutputStream out = new FileOutputStream(file);
		StringBuffer buf = new StringBuffer();
		Set<String> metaData = colName.keySet();
		Iterator<String> it = metaData.iterator();

		if (file.exists()) {

			buf.append(colName.size() + "|");
			String[] columnOrder = new String[colName.size()];
			int i = 0;
			while (it.hasNext()) {
				String key = it.next();
				columnOrder[i++] = key;
				buf.append(key + "|" + colName.get(key) + "|");
			}

			buf.append("\n");
			out.write(buf.toString().getBytes());

			StringBuffer tableContent = new StringBuffer();
			Iterator<Integer> keys = rows.keySet().iterator();

			while (keys.hasNext()) {
				
				Integer key = keys.next();
				Map<String, String> row = this.rows.get(key);
				tableContent.append(key + "|");
				for (i = 0; i < colName.size(); i++) {
					tableContent.append(row.get(columnOrder[i]) + "|");
				}

				tableContent.append("\n");
			}

			out.write(tableContent.toString().getBytes());
			// return tableContent.toString();

		}

		out.close();
	}

	public void dumpAllIndex()
	{
		Set<String> st=this.indexList.keySet();

		Iterator<String> it=st.iterator();

		while(it.hasNext())
		{
			String key=it.next();
			Index obj=this.indexList.get(key);
			obj.dumpIndex();
		}

	}


	public int insertRecord(Map<String, String> record) {

		if (record.size() != this.colName.size()) {
			String error = "Error: Size of record doesnt match size of columns in DataFile";
			throw new IllegalArgumentException(error);
		}

		/*
		 * Now check whether the record contains valid column Names and the size
		 * of the record values are with in the valid range
		 */
		Set<String> recordCol = record.keySet();
		Iterator<String> recIt = recordCol.iterator();

		while (recIt.hasNext()) {
			String recColName = recIt.next();
			if (!this.colName.containsKey(recColName)) {
				/*
				 * Record has a column name which is not supported by this
				 * DataFile
				 */
				String error = "Error: Record contains a column which is not in this DataFile";
				throw new IllegalArgumentException(error);
			}

			Integer colLength = this.colName.get(recColName);
			String values = record.get(recColName);
			if (values != null) {
				if (colLength < record.get(recColName).length()) {
					String error = "Error: Record contains a column whose length is greater than allowed";
					throw new IllegalArgumentException(error);
				}
			}
		}
		/*
		 * All isz Well. Now insert the record to data File and return the
		 * record id
		 */
		
		this.rows.put(this.id++, record);

		/*
		 * Add the record to index and update the index for this column.
		 */

		recordCol = record.keySet();
		recIt = recordCol.iterator();

		while (recIt.hasNext()) {

			String recColumn = recIt.next();

			Collection<Index> indexs = this.indexList.values();
			Iterator<Index> it = indexs.iterator();

			while (it.hasNext()) {
				Index temp = it.next();
				if (temp.getColumn().equals(recColumn)) {
					String value = record.get(recColumn);
					temp.insertValue((this.id - 1),
							value);
				}
			}
		}

		return (this.id - 1);
	}

	/**
	 * Description: Prints the contents of the datafile in human-readable format
	 * 
	 * @return String
	 */
	public String viewFile() {
		/* initialize record counter */
		int recordCounter = 0;

		StringBuffer tableContent = new StringBuffer();
		Iterator<Integer> rowIterator = rows.keySet().iterator();

		while (rowIterator.hasNext()) {
			Integer id = rowIterator.next();
			Map<String, String> rowElement = rows.get(id);
			tableContent.append(Integer.toString(id) + ":\n");
			tableContent.append(DataManager.print(rowElement));
			recordCounter++;
		}

		return tableContent.toString();

	}

	public void dropFile() {
		/* to delete file */

		File delFile = new File("data/"+fileName+"/"+fileName);
		if (!delFile.exists())
			throw new IllegalArgumentException(
			"DataFile::dropFile() => File does not exist");


		try{
			if (delFile.delete()) {
				System.out.println("DataFile::dropFile() => DataFile Sucessfully deleted");
			} else {
				throw new IllegalArgumentException(
				"DataFile::dropFile() => DataFile deletion failed !");
			}
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
		}

		/*delete index here */

		/* Remove table from DATA MANAGER as well */

		DataManager.removeTable(this.fileName);

		/* <<<<<<<<<<<<<<< delete INDEX here >>>>>>>>>>>>>>>>>>>>> */

		Enumeration<String> indexes=this.indexList.keys();

		while(indexes.hasMoreElements())
		{
			this.dropIndex(indexes.nextElement());
		}




	}

	public Map<String,String> getRecord(int recordId)
	{
		Map<String, String> resRow = null;
		try{
			resRow = rows.get(recordId);
		}catch(IndexOutOfBoundsException e){
			return null;
		}

		return resRow;

	}

	/*
	 * drop Index both on file and on disk
	 */
	public void dropIndex(String indexName)
	{
		/* if indexList contains this index then drop it */
		if(this.indexList.containsKey(indexName)){

			/* remove from memory */
			this.indexList.remove(indexName);

			/* remove from file */
			String indexFile="data/"+this.fileName+"/"+"Index/"+indexName;
			File delFile = new File(indexFile);
			if(delFile.exists()){
				if (delFile.delete()) {
					System.out.println("DataFile::dropIndex() => Index file : "+indexName+" Sucessfully deleted");
				} else {
					throw new IllegalArgumentException(
					"DataFile::dropIndex() => DataFile dropIndex failed !");
				}
			}

		}

	}

	/*
	 * restore Index
	 * 
	 */

	public Index restoreIndex(String indexName)
	{
		/* check if index file is on disk */
		String indexFileName="data/" + this.fileName+"/"+"Index/"+indexName;
		Index obj=null;
		int totIndexRows=0;
		File indexFile=new File(indexFileName);
		if(!indexFile.exists())
		{
			/* if index file does not exist on disk throw error */
			throw new IllegalArgumentException("DataFile::restoreIndex() => Index "+indexName+" file does not exist !");
		}

		/* if index exists in memory */
		if(this.indexList.containsKey(indexName))
		{
			throw new IllegalArgumentException("DataFile::restoreIndex() => Index already exists !");
		}


		/* now check if index is OUT OF SYNC with the file */

		/* go through the index records and check if any VALUE has been MODIFIED in the actual DATAFILE */

		File directory = new File("data/"+this.fileName+"/Index/"+indexName);

		boolean exists = directory.exists();
		if(exists)
		{
			try{


				FileReader fr = new FileReader(directory);
				BufferedReader br = new BufferedReader(fr);

				String line = br.readLine();

				/* column name on which index is created */
				String columnName=line;

				/*
				 * 	skip first line , it is column Name on which index is created 
				 */
				while((line=br.readLine())!=null){

					// increment rows;
					totIndexRows++;

					StringTokenizer st = new StringTokenizer(line," ");
					String indexColumnValue=st.nextToken();
					int	recordId=Integer.parseInt(st.nextToken());

					Map<String, String> rec=this.getRecord(recordId);
					String tableColumnValue=getColumnValue(rec,columnName);

					if(tableColumnValue==null)
						throw new IllegalStateException("DataFile::restoreIndex() ==> Recreate Index, Data file has changed !");

					if(!tableColumnValue.equals(indexColumnValue))
						throw new IllegalStateException("DataFile::restoreIndex() ==> Recreate Index, Data file has changed !");

				}

				/* check if any new RECORDS has been added in the DATAFILE */
				if(rows.size()!=totIndexRows)
					throw new IllegalStateException("DataFile::restoreIndex() ==> Recreate Index, Data file has changed !");


				/* if ALL IZZZ WELL then create Index */

				obj=this.createIndex(indexName, columnName);
				fr.close();
				br.close();
				return obj;

			}
			catch(Exception e)
			{
				throw new IllegalArgumentException();
			}
		}
		else
			throw new IllegalArgumentException();

	}


	/*
	 * reads record Map and returns only column value
	 */
	public String getColumnValue(Map<String, String> record,String columnName)
	{


		Set<String> keys = record.keySet();
		Iterator<String> it = keys.iterator();

		while(it.hasNext()) {

			String key = it.next();
			if(key.equals(columnName))
			{
				String value = record.get(key);
				return value;
			}

		}

		return null;

	}

	/*
	 * deleteRow() function 
	 * called by Iterator remove 
	 */
	public void deleteRow(int rowId, Iterator<Integer> it)
	{
		/* Delete the row form data file */
		Map<String, String> rowMap = this.rows.get(rowId);
		
		Enumeration<Index> et = this.indexList.elements();
		if (it != null) {
			it.remove();
		} else {
			this.rows.remove(rowId);
		}
		while ( et.hasMoreElements()) {
			
			Index obj = et.nextElement();
			String columnName = obj.getColumn();
			String value = rowMap.get(columnName);
			if (value != null)  {
				obj.delete(obj.getRoot(), obj.getRoot(), value + " | " + rowId);
				obj.updateIterator();
			}
		}
		
	}

	public Iterator<Integer> iterator(){
		FileIterator fit = new FileIterator();
		return fit;

	}
	public Index getIndex(String indexName) {
		return this.indexList.get(indexName);
	}

	public Index getIndexWithColum(String Column) {
		Index obj = null;

		Enumeration<String> k = this.indexList.keys();
		while(k.hasMoreElements()) {
			obj = this.indexList.get(k.nextElement());
			if (obj.getColumn().compareTo(Column) == 0)
				return obj;
		}
		return obj;
	}

	// The variable modCount is used to keep track of whether or not multiple iterators are executing the same stuff
	public class FileIterator implements Iterator<Integer>
	{

		public Iterator<Integer> it;
		public Integer id;

		public FileIterator() {
			this.it = rows.keySet().iterator();
		}
		
		@Override
		public boolean hasNext() {
			return it.hasNext();
		}

		@Override
		public Integer next() {
			id = it.next();
			return id;
		}

		@Override
		public void remove() {
			deleteRow(id, this.it);
		}

	}
}
