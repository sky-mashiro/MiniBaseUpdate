package Phase3;

import java.io.FileNotFoundException;

/**
 * This class is used to extend Cartesian join operation to join two tuples for
 * the given inequality condition. Existing implementation in MiniBASE works for
 * equality condition of single predicate case. Task 2b - Extend the join
 * operation created in Task 2b for two predicates inequality join.
 * 
 * @author Group 15
 *
 */
public class Task2bSolution {
	public static void main(String argv[]) throws FileNotFoundException {
		
		long startTime = System.currentTimeMillis();
		Runtime runtime = Runtime.getRuntime();
	    long usedMemoryBefore = runtime.totalMemory() - runtime.freeMemory();
	    System.out.println("Used Memory before" + usedMemoryBefore);
		
		boolean status;
		JoinsDriverTask2bSolution joinsDriverTask2bSolution = new JoinsDriverTask2bSolution(
				"src/tests/R.txt", "src/tests/query_2b.txt");
		status = joinsDriverTask2bSolution.runTests();
		if (status != true) {
			System.out.println("Error ocurred during join tests in Task1bSolution");
		} else {
			System.out.println("Task2b join tests completed successfully");
		}
		
		long stopTime = System.currentTimeMillis();
		long elapsedTime = stopTime - startTime;
		System.out.println("elapsedTime: " + elapsedTime / 1000.0);
		long usedMemoryAfter = runtime.totalMemory() - runtime.freeMemory();
	    System.out.println("Memory increased:" + (usedMemoryAfter-usedMemoryBefore) / 1048576);
	}
}
