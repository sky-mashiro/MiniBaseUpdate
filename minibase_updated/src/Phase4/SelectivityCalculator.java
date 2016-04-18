package Phase4;

import java.io.FileNotFoundException;
import java.util.ArrayList;

public class SelectivityCalculator {
	
	private static ArrayList<Integer> value = new ArrayList<Integer>();
	private ArrayList<ArrayList<Integer>> bucketList = new ArrayList<ArrayList<Integer>>();
	private static int bucketNumber = 10;
	
	public SelectivityCalculator()
	{
		value = new ArrayList<Integer>();
	}

	public void Calculator(ArrayList<Integer> R, ArrayList<Integer> S) 
	{
		
		
		
		int prev = S.get(0);
		int barCount = 0;
		int valueCount = 1;
		ArrayList<Integer> bucket = new ArrayList<>();
		value.add(prev);
		
		for(int i = 1; i<S.size(); i++)
		{
			if(S.get(i) != prev)
			 {
				 barCount++;
				 value.add(S.get(i));
				 bucket.add(valueCount);
				 valueCount = 1;
				 prev = S.get(i);
				 
				 if(barCount == bucketNumber)
				 {
					 barCount = 0;
					 bucketList.add(bucket);
					 System.out.println(bucket.toString());
					 System.out.println(bucket.size());
					 bucket = new ArrayList<>();
				 }	 
			 }
			 else
			 {
				 valueCount++;
			 }
		}
		
		bucket.add(valueCount);
		if(bucket.size() != 0)
		{
			bucketList.add(bucket);
			System.out.println(bucketList.get(0));
		}
		
		//System.out.println(bucketList.size());
		//System.out.println(value.size());
	}
	
	public double selectivityCal(ArrayList<Integer> SampleR, ArrayList<Integer> S, int op) {
		double selectivity = 0.0;
		//less than
		if (op == 1) {
			int index = 0;
			int numberOfBucket = 0;
			int rem = 0;
			double sum = 0.0;
			for (int r : SampleR) {
				for (int v : value) {
					if (r > v) {
						index = value.indexOf(v);
						numberOfBucket = index / 10;
						rem = index % 10;
						int i;
						for (i = 0; i < numberOfBucket; i++) {
							//System.out.println(bucketList.get(i).toString());
							for (int j = 0; j < bucketList.get(i).size(); j++) {
								System.out.println(sum);
								sum += bucketList.get(i).get(j);
							}
						}
						int remSum = 0;
						for (int k = 0; k < bucketList.get(i).size(); k++) {
							remSum += bucketList.get(i).get(k);
						}
						sum += (remSum * (rem+1) / 10.0);
						//System.out.println(sum);
						//System.out.println(sum);
						//System.out.println(SampleR.size());
						//System.out.println(S.size());
						
					}
					
				break;
				}
				//total += sum;
				//sum = 0;
			}
			selectivity = sum / (SampleR.size() * S.size());
		} else if (op == 0){//greater than
			int index = 0;
			int numberOfBucket = 0;
			int rem = 0;
			int remBucketIndex;
			double sum = 0.0;
			for (int r : SampleR) {
				for (int v : value) {
					if (r < v) {
						index = value.indexOf(v);
						rem = index % 10;
						remBucketIndex = index / 10;
						int i;
						for (i = remBucketIndex + 1; i < bucketList.size(); i++) {
							for (int j = 0; j < bucketList.get(i).size(); j++) {
								sum += bucketList.get(i).get(j);
							}
						}
						int remSum = 0;
						for (int k = 0; k < bucketNumber; k++) {
							remSum += bucketList.get(remBucketIndex).get(k);
						}
						sum += remSum * (bucketNumber - rem) / 10.0;
						selectivity = sum / (SampleR.size() * S.size());
					}
				}
			break;
			}
			
		} else {
			
		}
		return selectivity;
	}

	public static void main(String args[]) throws FileNotFoundException {
		//ArrayList<Integer> sampleR = Sampling.readCSV("src/Phase4/F1r.csv", 0.2, "1000");
		//ArrayList<Integer> sampleS = Sampling.readCSV("src/Phase4/F2r.csv", 0.1, "1000");
		Sampling sample = new Sampling("src/Phase4/F1r.csv", "src/Phase4/F2r.csv", 0.02, 0.1, "10000", "9000");
		System.out.println(sample.SampleR.size()); 
//		System.out.println(sample.RSort.size());
//		System.out.println(sample.SampleS.size());
//		System.out.println(sample.SSort.toString());
		
		SelectivityCalculator xo = new SelectivityCalculator();
		
		xo.Calculator(sample.SampleR, sample.SampleR);
		
		double res = xo.selectivityCal(sample.SampleR, sample.SampleR, 1);
		System.out.println(res);
		System.out.println(sample.SampleR.size());
		System.out.println(value.size());
//		System.out.println(value.toString());
	}

}
