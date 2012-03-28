package test;

import java.lang.reflect.Field;

import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import database.DataManager;

import org.junit.*;
import org.junit.runners.Suite;
import org.junit.runner.RunWith;


@RunWith(Suite.class)
@SuiteClasses({DMTests.class, DFTests.class, ITests.class})
public class SC {

	static StringBuilder score = new StringBuilder();
	static int totalScore;

	@BeforeClass
	public static void setUp() {
		score.append("Score:");
	}

	@AfterClass
	public static void tearDown() {
		System.out.println("\n\n"+score.toString()+"\n");
		
		int maxTotal = 0;
		try {
			for(Field f : Breakdown.class.getDeclaredFields()){
				maxTotal+=f.getInt(null);
			}
		} catch (Exception e) {
		}
		
		System.out.println("Total score: " + (DMTests.uscore + DFTests.dfscore + ITests.iscore) + "/" + maxTotal);

		System.out.println("\n@");
	}
}
