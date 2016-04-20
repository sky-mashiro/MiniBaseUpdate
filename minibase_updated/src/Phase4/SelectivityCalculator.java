package Phase4;

import java.io.FileNotFoundException;
import java.util.ArrayList;

public class SelectivityCalculator {
	
	private ArrayList<Integer> value = new ArrayList<Integer>();
	private ArrayList<Integer> bucketSize = new ArrayList<>();
	private ArrayList<ArrayList<Integer>> bucketList = new ArrayList<ArrayList<Integer>>();
	private int bucketNumber = 10; // For samll dataset, set it to 1
	
	public SelectivityCalculator()
	{
		value = new ArrayList<Integer>();
	}

	public void Calculator(ArrayList<Integer> S) //To get the buckets
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
		
		for(int m = 0; m<bucketList.size(); m++)
		{
			int sum = 0;
			for(int n = 0; n<bucketList.get(m).size(); n++)
			{
				sum += bucketList.get(m).get(n);
			}
			bucketSize.add(sum);
		}
		
		System.out.println(bucketSize.toString());
		//System.out.println(value.size());
	}
	
	public double selectivityCalculator(ArrayList<Integer> SampleR, ArrayList<Integer> S, int op)
	{
		double selectivity = 0d;
		
		if(op == 1)//R less than S
		{
			double selectCount = 0d;
			
			for(int r : SampleR)
			{
				int i = 0;
				for(; i<S.size(); i++)
				{
					if(r < S.get(i))
					{
						System.out.println("R: " + r + " S: " + S.get(i));
						break;
					}
				}
				int bucketNum = (int)(i/bucketNumber);
				int remain = i%bucketNumber;
				
				double bucketSum = 0;
				for(int j = bucketNum + 1; j < bucketSize.size(); j++)
				{
					bucketSum += bucketSize.get(j);
				}
				
				if(remain != 0 || bucketNum < bucketSize.size())
				{
					bucketSum += (bucketSize.get(bucketNum)*(bucketNumber-remain)/(double)bucketNumber);//If the last bucket is not full, we get an estimate count
				}
				
				selectCount += bucketSum;
			}
			
			selectivity = selectCount/(SampleR.size() * S.size());
		}
		
		if(op == 2)//R greater than S
		{
			double selectCount = 0d;
			
			for(int r : SampleR)
			{
				int i = 0;
				for(; i<S.size(); i++)
				{
					if(r <= S.get(i))
					{
						System.out.println("R: " + r + " S: " + S.get(i));
						break;
					}
				}
				
				int bucketNum = (int)(i/bucketNumber);
				int remain = i%bucketNumber;
				
				double bucketSum = 0;
				
				for(int j = 0; j < bucketNum; j++)
				{
					bucketSum += bucketSize.get(j);
				}
				
				System.out.println(bucketNum);
				
				if(remain != 0)
				{
					bucketSum += (bucketSize.get(bucketNum)*(remain)/(double)bucketNumber);
				}
				
				selectCount += bucketSum;
			}
			
			selectivity = selectCount/(SampleR.size() * S.size());
		}
		
		if(op == 0)//Extra credit R == S
		{
			
		}
		
		return selectivity;
	}
	

	public static void main(String args[]) throws FileNotFoundException {
		//ArrayList<Integer> sampleR = Sampling.readCSV("src/Phase4/F1r.csv", 0.2, "1000");
		//ArrayList<Integer> sampleS = Sampling.readCSV("src/Phase4/F2r.csv", 0.1, "1000");
		Sampling sampleCondition1 = new Sampling("src/tests/R2.txt", "src/tests/S2.txt", 1, 1, "4", "4");
		Sampling sampleCondition2 = new Sampling("src/tests/R2.txt", "src/tests/S2.txt", 1, 1, "4", "4");
		
		
		SelectivityCalculator xo1 = new SelectivityCalculator();
		SelectivityCalculator xo2 = new SelectivityCalculator();
		
		xo1.Calculator(sampleCondition1.SampleS);
		xo2.Calculator(sampleCondition2.SampleS);
		
		double res1 = xo1.selectivityCalculator(sampleCondition1.SampleR, sampleCondition1.SampleS, 1);
		double res2 = xo2.selectivityCalculator(sampleCondition2.SampleR, sampleCondition2.SampleS, 2);
		
		System.out.println(res1);
		System.out.println(res2);
//		System.out.println(sample.SampleR.size());
//		System.out.println(value.size());
//		System.out.println(value.toString());
	}

}
