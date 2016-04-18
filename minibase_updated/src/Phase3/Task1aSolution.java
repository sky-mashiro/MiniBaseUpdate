package Phase3;

import java.io.*;

/**
 * This class is used to extend Cartesian join operation to join two tuples for
 * the given inequality condition. Existing implementation in MiniBASE works for
 * equality condition of single predicate case. Task 1a - Extend the existing
 * single predicate equality join to single predicate inequality join.
 * 
 * @author Group 15
 *
 */


public class Task1aSolution {
	public static void main(String argv[]) throws FileNotFoundException {
		
		long startTime = System.currentTimeMillis();
		Runtime runtime = Runtime.getRuntime();
	    long usedMemoryBefore = runtime.totalMemory() - runtime.freeMemory();
	    System.out.println("Used Memory before" + usedMemoryBefore);
		
		boolean status;
		JoinsDriverTask1aSolution joinsDriverTask1aSolution = new JoinsDriverTask1aSolution(
				"src/tests/R.txt", "src/tests/S.txt", "src/tests/query_1a.txt");
		status = joinsDriverTask1aSolution.runTests();
		if (status != true) {
			System.out
					.println("Error ocurred during join tests in Task1aSolution");
		} else {
			System.out.println("Task1a join tests completed successfully");
		}
		
		long stopTime = System.currentTimeMillis();
		long elapsedTime = stopTime - startTime;
		System.out.println("elapsedTime: " + elapsedTime / 1000.0);
		long usedMemoryAfter = runtime.totalMemory() - runtime.freeMemory();
	    System.out.println("Memory increased:" + (usedMemoryAfter-usedMemoryBefore) / 1048576);

	}
}
