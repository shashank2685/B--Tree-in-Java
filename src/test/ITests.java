package test;

import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.junit.Test;

import database.DataFile;
import database.DataManager;
import database.Index;


public class ITests {

	static Map<String, Integer> desc ;
	static reference.DataFile df ;

	static int iscore = 0;

	static{
		desc = new HashMap<String, Integer>();
		desc.put("Title", 20);
		desc.put("Budget", 15);
		desc.put("Director", 20);
		desc.put("Year", 6);
		reference.DataManager.exit();
		df = reference.DataManager.restoreFile("movieVault");

	}


	@Test
	public void populateIndexUnique(){
		DataFile di1 = null;
		boolean eCaught = false;
		Map<Integer, Map<String, String>> rs = new HashMap<Integer, Map<String, String>>();


		int i = 0;
		Index i1 = null;


		try{
			di1 = DataManager.createFile("di1", desc);

			for(Map<String, String> r : df.records.values()){
				rs.put(di1.insertRecord(r), r);		
				if(i++ == 10){
					i1 = di1.createIndex("i1", "Title");
				}
			}

			String si1 = i1.viewIndex();

			int whenToStart = 0;
			int offset = 0;
			int count = 0;
			String prevKey = null;
			String key = null;
			for(String line : si1.split("\n")){
				if(whenToStart++ < 2)
					continue;
				if(whenToStart == 3){
					while(line.charAt(offset++) == '\t');
					offset--;
				}

				if(isLeaf(line, offset)){
					count++;
					line = line.trim();
					prevKey = key;
					key = line.split(" ")[0];
					int id = Integer.parseInt(line.split(" ")[1]);
					if(!rs.get(id).get("Title").equals(key)){
						throw new Exception();

					}
					if(prevKey != null)
						if(prevKey.compareTo(key) >= 0)
							throw new Exception();
				}
			}
			if(count != rs.size())
				throw new Exception();


		}catch(Exception e){
			System.out.println(e.getMessage());
			eCaught = true;
		}

		if(eCaught){
			SC.score.append("\n\t index isn't built correctly - 0/" + Breakdown.iIndexBuild);
		}else{
			iscore += Breakdown.iIndexBuild;
			SC.score.append("\n\t index is probably built correctly - " + +Breakdown.iIndexBuild + "/" + Breakdown.iIndexBuild);
		}
	}


	private boolean isLeaf(String s, int offset){
		for(int i = 0; i<offset; i++){
			if(s.charAt(i) != '\t')
				return false;
		}
		return true;
	}


	public void parseIndex(String in){
		Map<String, String> r = null;
		Map<Integer, Map<String, String>> ret = new HashMap<Integer, Map<String, String>>();
		int num = -1;

		String[] lines = in.split("\n");
		String[] tokens = null;
		for (String l : lines) {
			tokens = l.split(":");
			if(tokens.length < 2){
				if(num != -1){
					ret.put(num, r);
				}
				num = Integer.parseInt(tokens[0].trim());
				r = new HashMap<String, String>();
			}else{
				r.put(tokens[0].trim(), tokens[1].trim());	
			}
		}
		ret.put(num, r);		
	}

	@Test
	public void itFindUnique(){
		DataFile di2 = null;
		boolean eCaught = false;
		Map<Integer, Map<String, String>> rs1 = new HashMap<Integer, Map<String, String>>();
		Map<Integer, Map<String, String>> rs2 = new HashMap<Integer, Map<String, String>>();


		int i = 0;
		Index i2 = null;
		String title = null;

		try{
			di2 = DataManager.createFile("di2", desc);
			for(Map<String, String> r : df.records.values()){
				rs1.put(di2.insertRecord(r), r);	

				if(i++ == 10){
					i2 = di2.createIndex("i2", "Title");
				}
				if(i == 16){
					title = r.get("Title");
				}
			}

			Iterator<Integer> it = i2.iterator(title);
			int id = -1;
			int count = 0;
			while(it.hasNext()){
				it.hasNext();
				id = it.next();
				count++;
				rs2.put(id, di2.getRecord(id));
			}
			if(rs2.size() == 1 && rs2.get(id).equals(rs1.get(id)) && count == 1){
				iscore += Breakdown.iUniqueIteratorNext;
				SC.score.append("\n\t index iterator next() works correctly on index on unique-valued column - " + +Breakdown.iUniqueIteratorNext + "/" + Breakdown.iUniqueIteratorNext);
			}else{
				System.out.println(rs2.size());
				System.out.println(rs2.get(id));
				eCaught = true;
			}
		}catch(Exception e){
			eCaught = true;
		}

		if(eCaught){
			SC.score.append("\n\t index iterator next() doesn't work correctly on index on unique-valued column - 0/" + Breakdown.iUniqueIteratorNext);
		}
	}


	@Test
	public void itFindDuplicates(){
		DataFile di3 = null;
		boolean eCaught = false;
		Map<Integer,Map<String, String>> records = new HashMap<Integer,Map<String, String>>();

		Map<String, Set<Integer>> ids = new HashMap<String, Set<Integer>>();


		int i = 0;
		Index i3 = null;

		try{

			di3 = DataManager.createFile("di3", desc);
			i3 = di3.createIndex("i3", "Director");
			
			for(Map<String, String> r : df.records.values()){
				int id = di3.insertRecord(r);

				if(!ids.containsKey(r.get("Director")))
					ids.put(r.get("Director"), new TreeSet<Integer>());
				ids.get(r.get("Director")).add(id);

				records.put(id, r);
				if(i++ == 10){

				}
			}
			
			for(String key : ids.keySet()){
				Iterator<Integer> it = i3.iterator(key);
				int count = 0;
				//System.out.println(key);
				while(it.hasNext()){
					int id = it.next();
					//System.out.println(id);
					count++;
					if(!(ids.get(key).contains(id) && records.get(id).equals(di3.getRecord(id)))){
						eCaught = true;
						break;

					}
				}
				if(eCaught)
					break;

				if(count != ids.get(key).size()){
					System.out.println("count: " + count +"\n, size: " + ids.get(key).size() + "\n key: " + key);
					eCaught = true;
					break;
				}
			}
		}catch(Exception e){
			eCaught = true;
		}

		if(!eCaught){
			iscore += Breakdown.iDuplicateIteratorNext;
			SC.score.append("\n\t index iterator next() works correctly on index on duplicate-valued column - " + +Breakdown.iDuplicateIteratorNext + "/" + Breakdown.iDuplicateIteratorNext);
		}else{
			SC.score.append("\n\t index iterator next() doesn't work correctly on index on duplicate-valued column - 0/" + Breakdown.iDuplicateIteratorNext);
		}
	}





	@Test
	public void itRemove(){
		DataFile di4 = null;
		boolean eCaught = false;
		Map<Integer,Map<String, String>> records = new HashMap<Integer,Map<String, String>>();

		Map<String, Set<Integer>> ids = new HashMap<String, Set<Integer>>();

		Set<String> alreadyRemoved = new HashSet<String>();
		Index i4 = null;

		try{
			di4 = DataManager.createFile("di4", desc);
			i4 = di4.createIndex("i4", "Director");
			//building index (inserting records)
			for(Map<String, String> r : df.records.values()){
				int id = di4.insertRecord(r);			
				if(!ids.containsKey(r.get("Director")))
					ids.put(r.get("Director"), new TreeSet<Integer>());
				ids.get(r.get("Director")).add(id);
				records.put(id, r);
			}

			//removing single key
			String removedKey = df.records.get(df.records.size()/4).get("Director");
			Iterator<Integer> it = i4.iterator(removedKey);
			while(it.hasNext()){
				Integer id = it.next();
				it.remove();
				ids.get(removedKey).remove(id);
				records.remove(id);
			}

			it = i4.iterator(removedKey);
			alreadyRemoved.add(removedKey);

			//checking if all occurences removed
			int count = 0;
			while(it.hasNext()){
				it.next();
				count++;
			}
			if(count!=0)
				throw new Exception();

			//checking if all other keys are still there
			for(String key : ids.keySet()){
				it = i4.iterator(key);
				count = 0;
				while(it.hasNext()){
					int id = it.next();
					count++;
					if(!(ids.get(key).contains(id) && records.get(id).equals(di4.getRecord(id)))){
						throw new Exception();

					}
				}
				if(eCaught)
					break;

				if(count != ids.get(key).size()){
					//System.out.println("count: " + count +"\n size: " + ids.get(key).size() + "\n key: " + key);
					throw new Exception();
				}
			}

			//remove all other keys
			for(String key : ids.keySet()){
				alreadyRemoved.add(key);
				it = i4.iterator(key);
				count = 0;
				while(it.hasNext()){
					it.next();
					it.remove();
					count++;

					//checking if after removal other keys are still there
					for(String key2 : ids.keySet()){
						int count2 = 0;
						if(!alreadyRemoved.contains(key2)){
							Iterator<Integer> it2 = i4.iterator(key2);
							while(it2.hasNext()){
								it2.next();
								count2++;
							}

							if(count2 != ids.get(key2).size()){
								throw new Exception();
							}
						}
					}
				}
				if(count != ids.get(key).size()){
					//System.out.println("STEP 2 count: " + count +"\n size: " + ids.get(key).size() + "\n key: " + key);
					i4.viewIndex();
					throw new Exception();
				}
			}	

			//checking if index empty - more precisely if doesn't contain any indexes previously inserted
			for(String key : ids.keySet()){
				it = i4.iterator(key);
				count = 0;
				while(it.hasNext()){
					it.next();
					count++;
				}
				if(count != 0)
					throw new Exception();
			}

		}catch(Exception e){
			eCaught = true;
		}


		if(!eCaught){
			iscore += Breakdown.iRemove;
			SC.score.append("\n\t index iterator remove() works correctly on index on duplicate-valued column - " + +Breakdown.iRemove + "/" + Breakdown.iRemove);
		}else{
			SC.score.append("\n\t index iterator remove() doesn't work correctly on index on duplicate-valued column - 0/" + Breakdown.iRemove);
		}
	}


	@Test
	public void itUniqueRemove(){
		DataFile di5 = null;
		boolean eCaught = false;
		Map<Integer,Map<String, String>> records = new HashMap<Integer,Map<String, String>>();

		Map<String, Set<Integer>> ids = new HashMap<String, Set<Integer>>();

		Set<String> alreadyRemoved = new HashSet<String>();
		Index i5 = null;

		try{
			di5 = DataManager.createFile("di5", desc);
			i5 = di5.createIndex("i5", "Title");
			//building index (inserting records)
			int i = 0;
			for(Map<String, String> r : df.records.values()){
				i++;
				//if (i == 500)
					//break;
				int id = di5.insertRecord(r);			
				if(!ids.containsKey(r.get("Title")))
					ids.put(r.get("Title"), new TreeSet<Integer>());
				ids.get(r.get("Title")).add(id);
				records.put(id, r);
			}

			//removing single key
			String removedKey = df.records.get(df.records.size()/4).get("Title");
			Iterator<Integer> it = i5.iterator(removedKey);
			while(it.hasNext()){
				Integer id = it.next();
				it.remove();
				ids.get(removedKey).remove(id);
				records.remove(id);
			}

			it = i5.iterator(removedKey);
			alreadyRemoved.add(removedKey);

			//checking if all occurences removed
			int count = 0;
			while(it.hasNext()){
				it.next();
				count++;
			}
			if(count!=0)
				throw new Exception();

			//checking if all other keys are still there
			
			for(String key : ids.keySet()){
				it = i5.iterator(key);
				count = 0;
				while(it.hasNext()){
					int id = it.next();
					count++;
					if(!(ids.get(key).contains(id) && records.get(id).equals(di5.getRecord(id)))){
						throw new Exception();

					}
				}
				if(eCaught)
					break;

				if(count != ids.get(key).size()){
					//System.out.println("count: " + count +"\n size: " + ids.get(key).size() + "\n key: " + key);
					throw new Exception();
				}
			}
		
			//remove all other keys
			i = 0;
			for(String key : ids.keySet()){
				alreadyRemoved.add(key);
				it = i5.iterator(key);
				count = 0;
				while(it.hasNext()){
					it.next();
					//System.out.print(i + " : ");
					it.remove();
					count++;
					
					i++;		
					//System.out.println(i5.viewIndex());
					//checking if after removal other keys are still there
		
					for(String key2 : ids.keySet()){
						int count2 = 0;
						//System.out.println(key2);
						
						if(!alreadyRemoved.contains(key2)){
							Iterator<Integer> it2 = i5.iterator(key2);
							while(it2.hasNext()){
								it2.next();
								count2++;
							}
							
							if(count2 != ids.get(key2).size()){
								throw new Exception();
							}
						}
					}
				}
				if(count != ids.get(key).size()){
					System.out.println("STEP 2 count: " + count +"\n size: " + ids.get(key).size() + "\n key: " + key);
					throw new Exception();
				}
			}	
			System.out.println("I am here 1 .....");
			//checking if index empty - more precisely if doesn't contain any indexes previously inserted
			for(String key : ids.keySet()){
				it = i5.iterator(key);
				count = 0;
				while(it.hasNext()){
					System.out.println(i5.viewIndex());
					it.next();
					count++;
				}
				if(count != 0)
					throw new Exception();
			}


			System.out.println("I am here 2 ...... ");
		

		}catch(Exception e){
			eCaught = true;
		}


		if(!eCaught){
			iscore += Breakdown.iUniqueRemove;
			SC.score.append("\n\t index iterator remove() works correctly on index on unique-valued column - " + +Breakdown.iUniqueRemove + "/" + Breakdown.iUniqueRemove);
		}else{
			SC.score.append("\n\t index iterator remove() doesn't work correctly on index on unique-valued column - 0/" + Breakdown.iUniqueRemove);
		}
	}

	//checks if removing record from index updates other index etc.
	@Test
	public void iConcurrency(){
		DataFile di6 = null;
		boolean eCaught = false;
		Map<Integer,Map<String, String>> records = new HashMap<Integer,Map<String, String>>();

		Map<String, Set<Integer>> ids = new HashMap<String, Set<Integer>>();

		Set<String> titles = new LinkedHashSet<String>();
		Set<String> directors = new LinkedHashSet<String>();
		Index i6d = null;
		Index i6t = null;

		try{
			di6 = DataManager.createFile("di6", desc);

			int i = 0;
			//building index (inserting records)
			for(Map<String, String> r : df.records.values()){
				i++;

				int id = di6.insertRecord(r);			


				if(i > df.records.size()*3/11 && i6d == null)
					i6d = di6.createIndex("i6d", "Director");

				if(i > df.records.size()*7/11 && i6t == null)
					i6t = di6.createIndex("i6t", "Title");	

				titles.add(r.get("Title"));
				directors.add(r.get("Director"));


				//if(!ids.containsKey(r.get("Title")))
				//ids.put(r.get("Title"), new TreeSet<Integer>());
				//ids.get(r.get("Title")).add(id);
				//records.put(id, r);
			}

			Iterator<Integer> itF = di6.iterator();
			Iterator<Integer> itD;
			Iterator<Integer> itT; 

			int all = 0;
			while(itF.hasNext()){
				itF.next();
				all++;
			}
			if(all != df.records.size())
				throw new Exception();

			int count = 0;
			for(String key : directors){
				itD = i6d.iterator(key);
				while(itD.hasNext()){
					itD.next();
					count++;
				}
			}
			if(count != all)
				throw new Exception();

			count = 0;
			for(String key : titles){
				itT = i6t.iterator(key);
				while(itT.hasNext()){
					itT.next();
					count++;
				}
			}
			if(count != all)
				throw new Exception();


			Map<String, String> recordToRemove = df.records.get(df.records.size()*3/4);

			itD = i6d.iterator(recordToRemove.get("Director"));
			i=0;
			while(!di6.getRecord(itD.next()).equals(recordToRemove) && i<all){
				i++;
			}
			if(i==all)
				throw new Exception();

			itD.remove();

			itT = i6t.iterator(recordToRemove.get("Title"));
			count = 0;
			while(itT.hasNext()){
				itT.next();
				count++;
			}
			if(count != 0){
				//System.out.println("ID= ", di6.records.)
				DataManager.print(recordToRemove);

				System.out.println("Count: " + count);




				throw new Exception();
			}
			all = all-1; //all means number of records in the file, it just decreased bc of deletion


			recordToRemove = df.records.get(df.records.size()/2);
			di6.insertRecord(recordToRemove);
			try{
				itT = i6t.iterator(recordToRemove.get("Title"));
				itD = i6d.iterator(recordToRemove.get("Director"));
				itT.next();
				itT.remove();
				itD.next();
			}catch(ConcurrentModificationException cme){
				eCaught = true; //expected
			}

			if(!eCaught){
				throw new Exception();
			}
			eCaught = false;

			i = 0;
			itF = di6.iterator();
			while(itF.hasNext() && i<all/2){
				itF.next();
				itF.remove();
				i++;
			}
			all-= all/2;

			count = 0;
			for(String key : directors){
				itD = i6d.iterator(key);
				while(itD.hasNext()){
					itD.next();
					count++;
				}
			}
			if(count != all)
				throw new Exception();

			count = 0;
			for(String key : titles){
				itT = i6t.iterator(key);
				while(itT.hasNext()){
					itT.next();
					count++;
				}
			}
			if(count != all)
				throw new Exception();


			i=0;
			for(String key : directors){
				if(i>=all/2)
					break;
				itD = i6d.iterator(key);
				while(itD.hasNext() && i < all/2){
					itD.next();
					itD.remove();
					i++;
				}
			}
			all-=all/2;

			count = 0;
			for(String key : titles){
				itT = i6t.iterator(key);
				while(itT.hasNext()){
					itT.next();
					count++;
				}
			}
			if(count != all){
				System.out.println("all: " + all + " count: " + count);
				throw new Exception();
			}

			count = 0;
			itF = di6.iterator();
			while(itF.hasNext()){
				itF.next();
				count++;
			}
			if(count != all)
				throw new Exception();

		}catch(Exception e){
			System.out.println(e.getClass());
			eCaught = true;
		}


		if(!eCaught){
			iscore += Breakdown.iConcurrency;
			SC.score.append("\n\t synchronizing file and indexes after updates works correctly - " + +Breakdown.iConcurrency + "/" + Breakdown.iConcurrency);
		}else{
			SC.score.append("\n\t synchronizing file and indexes after updates doesn't work correctly - 0/" + Breakdown.iConcurrency);
		}
	}

}
