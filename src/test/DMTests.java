package test;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import database.DataFile;
import database.DataManager;

public class DMTests {

	static int uscore = 0;

	@Test
	public void createFile() {
		boolean eCaught = false;
		DataFile df1 = null;

		Map<String, Integer> descriptor1 = new HashMap<String, Integer>();
		descriptor1.put("Title", 20);
		descriptor1.put("Budget", 15);
		descriptor1.put("Director", 20);
		descriptor1.put("Year", 6);
		try {
			df1 = DataManager.createFile("test1", descriptor1);
		} catch (Exception e) {
			eCaught = true;
		}

		if (eCaught || df1 == null) {
			SC.score.append("\n\t createFile doesn't work - 0/" + Breakdown.dmCreateFileExists);
			SC.score.append("\n\t createFile doesn't throw IllegalStateException correctly - 0/" + Breakdown.dmCreateFileException);
			return;
		} else {
			SC.score.append("\n\t createFile works - " + Breakdown.dmCreateFileExists + "/" + Breakdown.dmCreateFileExists);
			uscore += Breakdown.dmCreateFileExists;
		}

		DataFile df2;
		eCaught = false;

		try {
			df2 = DataManager.createFile("test1", descriptor1);
		} catch (IllegalArgumentException e) {
			eCaught = true;
		} catch (Exception e) {
			// fall through
		}

		if (eCaught) {
			uscore += Breakdown.dmCreateFileException;
			SC.score.append("\n\t createFile throws exception correctly - " + +Breakdown.dmCreateFileException + "/" + Breakdown.dmCreateFileException);
		} else {
			SC.score.append("\n\t createFile doesn't throw IllegalStateException - 0/" + Breakdown.dmCreateFileException);
		}
	}
	
	@Test
	public void restoreFile() {
		boolean eCaught = false;
		DataFile df1 = null;
		DataFile df2 = null;

		Map<String, Integer> descriptor1 = new HashMap<String, Integer>();
		descriptor1.put("Title", 20);
		descriptor1.put("Budget", 15);
		descriptor1.put("Director", 20);
		descriptor1.put("Year", 6);

		try {
			df1 = DataManager.createFile("test2", descriptor1);

			DataManager.exit();

			df2 = DataManager.restoreFile("test2");
		} catch (Exception e) {
			eCaught = true;
		}

		if (eCaught || df2 == null) {
			SC.score.append("\n\t restoreFile or exit doesn't work - 0/" + Breakdown.dmRestoreExit);
			SC.score.append("\n\t restoreFile doesn't throw IllegalStateException correctly - 0/" + Breakdown.dmRestoreException);
			return;
		} else {
			SC.score.append("\n\t restoreFile and exit probably work - " + Breakdown.dmRestoreExit + "/" + Breakdown.dmRestoreExit);
			uscore += Breakdown.dmRestoreExit;
		}
		
		eCaught = false;

		try {
			df2 = DataManager.restoreFile("test3");
		} catch (IllegalArgumentException e) {
			eCaught = true;
		} catch (Exception e) {
			// fall through
		}

		if (eCaught) {
			uscore += Breakdown.dmRestoreException;
			SC.score.append("\n\t restoreFile throws exception correctly - " + +Breakdown.dmRestoreException + "/" + Breakdown.dmRestoreException);
		} else {
			SC.score.append("\n\t restoreFile doesn't throw IllegalStateException - 0/" + Breakdown.dmRestoreException);
		}
	}

	@Test
	public void print() {
		boolean eCaught = false;
		Map<String, String> r1 = new HashMap<String, String>();
		r1.put("Title", "Star Wars");
		r1.put("Budget", "1000000");
		r1.put("Director", "Lucas");
		r1.put("Year", "1960");
		String output = null;
		try {
			output = DataManager.print(r1);
		} catch (Exception e) {
			eCaught = true;
		}

		if (eCaught) {
			SC.score.append("\n\t print incorrert - 0/" + Breakdown.dmPrint);
		} else {
			try {
				if (r1.equals(parseRecord(output))) {
					SC.score.append("\n\t print correct - " + Breakdown.dmPrint + "/" + Breakdown.dmPrint);
					uscore += Breakdown.dmPrint;
				} else {
					SC.score.append("\n\t print incorrert - 1/" + Breakdown.dmPrint);
					uscore++;
				}
			} catch (Exception e) {
				SC.score.append("\n\t print incorrert - 0/" + Breakdown.dmPrint);
			}
		}
	}

	static Map<String, String> parseRecord(String in) {
		Map<String, String> r = new HashMap<String, String>();

		String[] lines = in.split("\n");
		for (String l : lines) {
			String[] tokens = l.split(": ");
			r.put(tokens[0].trim(), tokens[1].trim());
		}
		return r;
	}



}
