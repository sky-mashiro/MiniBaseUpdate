package Phase3;

import java.io.FileNotFoundException;

/**
 * This class is used to extend Cartesian join operation to join two tuples for
 * the given inequality condition. Existing implementation in MiniBASE works for
 * equality condition of single predicate case. Task 2c - Extend the join
 * operation created in Task 2c for two predicates inequality join.
 * 
 * @author Group 15
 *
 */
public class Task2dSolutionc {
	public static void main(String argv[]) throws FileNotFoundException {
		
		long startTime = System.currentTimeMillis();
		Runtime runtime = Runtime.getRuntime();
	    long usedMemoryBefore = runtime.totalMemory() - runtime.freeMemory();
	    System.out.println("Used Memory before" + usedMemoryBefore);
		
		boolean status;

		JoinsDriverTask2dSolutionc joinsDriverTask2dSolutionc = new JoinsDriverTask2dSolutionc(
				"src/tests/R.txt", "src/tests/Q2000.txt", "src/tests/query_2c_2.txt");
		status = joinsDriverTask2dSolutionc.runTests();
		if (status != true) {
			System.out
					.println("Error ocurred during join tests in Task2cSolution");
		} else {
			System.out.println("Task2c join tests completed successfully");
		}
		
		long stopTime = System.currentTimeMillis();
		long elapsedTime = stopTime - startTime;
		System.out.println("elapsedTime: " + elapsedTime / 1000.0);
		long usedMemoryAfter = runtime.totalMemory() - runtime.freeMemory();
	    System.out.println("Memory increased:" + (usedMemoryAfter-usedMemoryBefore) / 1048576);

	}
}
