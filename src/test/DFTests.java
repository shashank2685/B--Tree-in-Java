package test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.junit.Test;

import database.DataManager;
import database.DataFile;
import database.Index;

public class DFTests {
	
	
	static Map<String, Integer> desc ;
	static reference.DataFile df ;

	static int dfscore = 0;
	
	static{
		desc = new HashMap<String, Integer>();
		desc.put("Title", 20);
		desc.put("Budget", 15);
		desc.put("Director", 20);
		desc.put("Year", 6);
		df = reference.DataManager.restoreFile("movieVault");
	}
	
	
	
	@Test
	public void InsRec(){
		boolean eCaught = false;
		
		DataFile df1 = null;
		try{
			df1 = DataManager.createFile("df1", desc);
			int id = df1.insertRecord(df.getRecord(0));
			if(df1.getRecord(id).equals(df.getRecord(0))){
				dfscore+=Breakdown.dfInsertRecord ;
				SC.score.append("\n\t insertRecord/getRecord work - " + Breakdown.dfInsertRecord + "/" + Breakdown.dfInsertRecord);
			}else{
				eCaught = true;
			}
		}catch(Exception e){
			eCaught = true;
		}
		
		if(eCaught){
			SC.score.append("\n\t insertRecord/getRecord don't work - 0/" + Breakdown.dfInsertRecord);
		};
		
		eCaught = false;
		try{
			Map<String, String> r = new HashMap<String, String>();
			r.put("Title", "Star Wars");
			r.put("Budgget", "a lot");
			r.put("Director", "Lucas");
			r.put("Year", "1111");
			
			df1.insertRecord(r);
			
		}catch (IllegalArgumentException e) {
			eCaught = true;
		} catch (Exception e) {
			// fall through
		}
		
		if (eCaught) {
			dfscore += Breakdown.dfInsertRecordColName;
			SC.score.append("\n\t insertRecord checks column name correctly - " + +Breakdown.dfInsertRecordColName + "/" + Breakdown.dfInsertRecordColName);
		} else {
			SC.score.append("\n\t insertRecord doesn't check column name correctly - 0/" + Breakdown.dfInsertRecordColName);
		}
		
		eCaught = false;
		try{
			Map<String, String> r = new HashMap<String, String>();
			r.put("Title", "Star Warsbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
			r.put("Budget", "a lot");
			r.put("Director", "Lucas");
			r.put("Year", "1111");
			
			df1.insertRecord(r);
			
		}catch (IllegalArgumentException e) {
			eCaught = true;
		} catch (Exception e) {
			// fall through
		}
		
		if (eCaught) {
			dfscore += Breakdown.dfInsertRecordLength;
			SC.score.append("\n\t insertRecord checks value length correctly - " + +Breakdown.dfInsertRecordLength + "/" + Breakdown.dfInsertRecordLength);
		} else {
			SC.score.append("\n\t insertRecord doesn't check value length correctly - 0/" + Breakdown.dfInsertRecordLength);
		}
	}
	
	@Test
	public void viewFile(){
		boolean eCaught = false;
		
		int i = 0;
		Map<Integer, Map<String, String>> rs1= new HashMap<Integer, Map<String, String>>();
		Map<Integer, Map<String, String>> rs2;
		
		DataFile df2 = null;
		
		try{
			df2 = DataManager.createFile("df2", desc);	
			for(Map<String, String> r : df.records.values()){
				rs1.put(df2.insertRecord(r), r);	
				if(++i > 10)
					break;
			}
			
			rs2 = parseFile(df2.viewFile());
			
			if(rs2.equals(rs1)){
				dfscore += Breakdown.dfViewFile;
				SC.score.append("\n\t viewFile works correctly - " + +Breakdown.dfViewFile + "/" + Breakdown.dfViewFile);	
			}else{
				eCaught = true;
			}
		}catch(Exception e){
			eCaught = true;
		}
		
		if(eCaught){
			SC.score.append("\n\t viewFile doesn't work correctly - 0/" + Breakdown.dfViewFile);
		}
	}
	
	
	
	
	
	@Test
	public void FilePersistence(){
		DataFile df3 = null;
		
		boolean eCaught = false;
		
		int i = 0;
		Map<Integer, Map<String, String>> rs1= new HashMap<Integer, Map<String, String>>();
		Map<Integer, Map<String, String>> rs2= new HashMap<Integer, Map<String, String>>();
		
		
		try{
			df3 = DataManager.createFile("df3", desc);	
			for(Map<String, String> r : df.records.values()){
				rs1.put(df3.insertRecord(r), r);	
				if(++i > 10)
					break;
			}
			
			df3.dumpFile();
			DataManager.exit();
			
			df3 = DataManager.restoreFile("df3");
			
			for(Integer j : rs1.keySet()){
				rs2.put(j, df3.getRecord(j));
			}
			
			
			if(rs2.equals(rs1)){
				dfscore += Breakdown.dfRestoreWithRecords;
				SC.score.append("\n\t file with records is saved and restored correctly - " + +Breakdown.dfRestoreWithRecords + "/" + Breakdown.dfRestoreWithRecords);	
			}else{
				eCaught = true;
			}
		}catch(Exception e){
			eCaught = true;
		}
		
		if(eCaught){
			SC.score.append("\n\t file with records isn't saved or restored correctly  - 0/" + Breakdown.dfRestoreWithRecords);
		}
	}
	
	
	@Test
	public void iterator(){
		DataFile df4 = null;
		
		boolean eCaught = false;
	
		Map<Integer, Map<String, String>> rs1 = new HashMap<Integer, Map<String, String>>();
		Map<Integer, Map<String, String>> rs2 = new HashMap<Integer, Map<String, String>>();
		
		int i = 0;
		try{
			
			
			df4 = DataManager.createFile("df4", desc);
			for(Map<String, String> r : df.records.values()){
				rs1.put(df4.insertRecord(r), r);	
				if(++i > 10)
					break;
			}
			
			Iterator<Integer> it = df4.iterator();
			while(it.hasNext()){
				int id = it.next();
				rs2.put(id, df4.getRecord(id));
			}
			if(rs1.equals(rs2)){
				dfscore += Breakdown.dfIteratorTraverse;
				SC.score.append("\n\t it is possible to traverse file using file iterator - " + +Breakdown.dfIteratorTraverse + "/" + Breakdown.dfIteratorTraverse);
			}else{
				eCaught = true;
			}	
		}catch(Exception e){
			eCaught = true;
		}
		
		if(eCaught){
			SC.score.append("\n\t traversal of file doeesn't work correctly  - 0/" + Breakdown.dfIteratorTraverse);
			SC.score.append("\n\t\t and therefore also remove() can't work properly  - 0/" + Breakdown.dfIteratorRemoveBasic);
		}else{
			eCaught = false;
			try{
				Iterator<Integer> it1 = df4.iterator();
				int count = 0;
				int id = 0;
				while(it1.hasNext()){
					id = it1.next();
					it1.remove();	
					count++;
				}
				if(count == rs1.size()){
					Iterator<Integer> it2 = df4.iterator();
					
					while(it2.hasNext()){
						it2.next();
					}
					if(count != rs1.size())
						eCaught = true;
					
					if (df4.getRecord(id) != null)
						eCaught = true;
					
					
				}else{
					eCaught = true;
				}
			}catch(Exception e){
				eCaught = true;
			}
			
			if(!eCaught){
				dfscore += Breakdown.dfIteratorRemoveBasic;
				SC.score.append("\n\t file iterator remove works (on basic level) - " + +Breakdown.dfIteratorRemoveBasic + "/" + Breakdown.dfIteratorRemoveBasic);
			}else{
				SC.score.append("\n\t file iterator remove() doesn't work correctly - 0/" + Breakdown.dfIteratorRemoveBasic);
			}
		}
	}
	
	private Map<Integer, Map<String, String>>  parseFile(String in){
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
		return ret;
	}
	
}
