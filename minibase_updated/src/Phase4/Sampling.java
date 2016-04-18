package Phase4;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Scanner;

public class Sampling {

	public ArrayList<Integer> RSort;
	public ArrayList<Integer> SSort;
	public ArrayList<Integer> SampleR;
	public ArrayList<Integer> SampleS;
	
	public Sampling(String filePathR, String filePathS, double percentR, double percentS, String attrR, String attrS) throws FileNotFoundException
	{
		RSort = new ArrayList<>();
		SSort = new ArrayList<>();
		SampleS = new ArrayList<>();
		SampleR = new ArrayList<>();
		readCSVR(filePathR, percentR, attrR);
		readCSVS(filePathS, percentS, attrS);
	}
	
	public void readCSVS(String filePath, double percent, String attr) throws FileNotFoundException {
		File file = new File(filePath);
		Scanner input = new Scanner(file);
		input.useDelimiter(",");

		String attrListString = input.nextLine();
		String[] attrList = attrListString.split(",");
		int i = 0;
		for (; i < attrList.length; i++) {
			if (attrList[i].equals(attr)) {
				break;
			}
		}

		int count = (int) (1 / percent);
		int line = 0;

		while (input.hasNextLine()) {
			line++;
			String x = input.nextLine();
			if (line % count == 0) {
				SampleS.add(Integer.parseInt(x.split(",")[i]));
				// System.out.println(line);
			}
			
			SSort.add(Integer.parseInt(x.split(",")[i]));
			
		}
		
		Collections.sort(SampleS);
		Collections.sort(SSort);
		
		input.close();
	}
	
	public void readCSVR(String filePath, double percent, String attr) throws FileNotFoundException {
		File file = new File(filePath);
		Scanner input = new Scanner(file);
		input.useDelimiter(",");

		String attrListString = input.nextLine();
		String[] attrList = attrListString.split(",");
		int i = 0;
		for (; i < attrList.length; i++) {
			if (attrList[i].equals(attr)) {
				break;
			}
		}

		int count = (int) (1 / percent);
		int line = 0;

		while (input.hasNextLine()) {
			line++;
			String x = input.nextLine();
			if (line % count == 0) {
				SampleR.add(Integer.parseInt(x.split(",")[i]));
				// System.out.println(line);
			}
			
			RSort.add(Integer.parseInt(x.split(",")[i]));
			
		}
		
		Collections.sort(SampleR);
		Collections.sort(RSort);
		
		input.close();
	}
}
