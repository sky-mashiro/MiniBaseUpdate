package Phase3;

import java.io.FileNotFoundException;

/**
 * This class is used to extend Cartesian join operation to join two tuples for
 * the given inequality condition. Existing implementation in MiniBASE works for
 * equality condition of single predicate case. Task 1b - Extend the join
 * operation created in Task 1b for two predicates inequality join.
 * 
 * @author Group 15
 *
 */
public class Task1bSolution {
	public static void main(String argv[]) throws FileNotFoundException {
		
		long startTime = System.currentTimeMillis();
		Runtime runtime = Runtime.getRuntime();
	    long usedMemoryBefore = runtime.totalMemory() - runtime.freeMemory();
	    System.out.println("Used Memory before" + usedMemoryBefore);
		
		boolean status;

		JoinsDriverTask1bSolution joinsDriverTask1bSolution = new JoinsDriverTask1bSolution(
				"src/tests/R.txt", "src/tests/S.txt", "src/tests/query_1b.txt");
		status = joinsDriverTask1bSolution.runTests();
		if (status != true) {
			System.out
					.println("Error ocurred during join tests in Task1bSolution");
		} else {
			System.out.println("Task1b join tests completed successfully");
		}
		
		long stopTime = System.currentTimeMillis();
		long elapsedTime = stopTime - startTime;
		System.out.println("elapsedTime: " + elapsedTime / 1000.0);
		long usedMemoryAfter = runtime.totalMemory() - runtime.freeMemory();
	    System.out.println("Memory increased:" + (usedMemoryAfter-usedMemoryBefore) / 1048576);

	}
}
