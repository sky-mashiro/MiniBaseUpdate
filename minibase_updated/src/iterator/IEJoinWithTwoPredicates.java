package iterator;

import heap.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import index.*;
import java.lang.*;
import java.util.HashMap;
import java.util.Map;

//import com.sun.javafx.webkit.KeyCodeMap.Entry;

import java.io.*;

import java.io.IOException;

import bufmgr.PageNotReadException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import index.IndexException;

public class IEJoinWithTwoPredicates extends Iterator{

	  private AttrType      _in1[];
	  private   int        in1_len;
	  private AttrType      _in2[];
	  private   int        in2_len;
	  private   Iterator  outer;
	  private   Iterator  inner;
	  private   Iterator  outer2;
	  private   Iterator  inner2;
	  private   CondExpr OutputFilter[];
	  private   CondExpr OutputFilter2[];
	  private   int        n_buf_pgs;        // # of buffer pages available.
	  private   boolean        done,         // Is the join complete
	    get_from_outer;                 // if TRUE, a tuple is got from outer
	  private   Tuple     outer_tuple, inner_tuple;
	  private   Tuple     Jtuple;           // Joined tuple
	  private   FldSpec   perm_mat[];
	  private   int        nOutFlds;
	  private   Heapfile  rhf;
	  private   Heapfile  shf;
	  private   String  RelationName;
	  private   String  RelationName2;
	  private   short[] str1_size;
	  private   short[] str2_size;
	  private   int comFld;
	  private HashMap<Integer,Integer> sortedTableR1;
	  private HashMap<Integer,Integer> sortedReverseR1;
	  private HashMap<Integer,Integer> sortedTableR1ID;
	  private HashMap<Integer,Integer> sortedTableR2;
	  private HashMap<Integer,Integer> sortedTableR2IDReverse;
	  private HashMap<Integer,Integer> sortedTableS1;
	  private HashMap<Integer,Integer> sortedTableS1ID;
	  private HashMap<Integer,Integer> sortedTableS2IDReverse;
	  private HashMap<Integer,Integer> sortedReverseR2;
	  private HashMap<Integer,Integer> sortedReverseS1;
	  private HashMap<Integer,Integer> sortedReverseS2;
	  private HashMap<Integer,Integer> sortedTableS2;
	  
	  /**constructor
	   *Initialize the two relations which are joined, including relation type,
	   *@param in1  Array containing field types of R.
	   *@param len_in1  # of columns in R.
	   *@param t1_str_sizes shows the length of the string fields.
	   *@param in2  Array containing field types of S
	   *@param len_in2  # of columns in S
	   *@param  t2_str_sizes shows the length of the string fields.
	   *@param amt_of_mem  IN PAGES
	   *@param am1  access method for left i/p to join
	   *@param relationName  access hfapfile for right i/p to join
	   *@param outFilter   select expressions
	   *@param rightFilter reference to filter applied on right i/p
	   *@param proj_list shows what input fields go where in the output tuple
	   *@param n_out_flds number of outer relation fileds
	   *@exception IOException some I/O fault
	   *@exception NestedLoopException exception from this class
	   */
	public IEJoinWithTwoPredicates(AttrType in1[], int len_in1, short t1_str_sizes[],
			String relationName,AttrType in2[], int len_in2, short t2_str_sizes[], int amt_of_mem,
			String relationName2, CondExpr outFilter[], CondExpr outFilter2[], FldSpec proj_list[], int n_out_flds)
			throws IOException, NestedLoopException {
		
		FldSpec[] rProjection = new FldSpec[len_in1];
		for (int i = 0; i < 4; i++) {
			rProjection[i] = new FldSpec(new RelSpec(RelSpec.outer), i+1);
			//System.out.println(Projection[i].offset);
		}

		FldSpec[] sProjection = new FldSpec[len_in1];
		for (int i = 0; i < 4; i++) {
			sProjection[i] = new FldSpec(new RelSpec(RelSpec.innerRel), i+1);
			//System.out.println(Projection[i].offset);
		}
		
		try {
			outer = new FileScan(relationName, in1, t1_str_sizes, (short) len_in1, len_in1, rProjection, null);
			inner = new FileScan(relationName2, in2, t2_str_sizes, (short) len_in2, len_in2, sProjection, null);
			outer2 = new FileScan(relationName, in1, t1_str_sizes, (short) len_in1, len_in1, rProjection, null);
			inner2 = new FileScan(relationName2, in2, t2_str_sizes, (short) len_in2, len_in2, sProjection, null);
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		sortedTableR1 = new HashMap<>();
		sortedTableR2 = new HashMap<>();
		sortedTableS1 = new HashMap<>();
		sortedTableS2 = new HashMap<>();
		sortedReverseR1 = new HashMap<>();
		sortedReverseR2 = new HashMap<>();
		sortedReverseS1 = new HashMap<>();
		sortedReverseS2 = new HashMap<>();
		sortedTableR1ID = new HashMap<>();
		sortedTableR2IDReverse = new HashMap<>();
		sortedTableS1ID = new HashMap<>();
		sortedTableS2IDReverse = new HashMap<>();
		
		RelationName = relationName;
		_in1 = new AttrType[in1.length];
		System.arraycopy(in1, 0, _in1, 0, in1.length);
		in1_len = len_in1;
		
		RelationName2 = relationName2;
		_in2 = new AttrType[in2.length];
		System.arraycopy(in2, 0, _in2, 0, in2.length);
		in2_len = len_in2;

		// outer = am1;
		inner_tuple = new Tuple();
		Jtuple = new Tuple();
		OutputFilter = outFilter;
		OutputFilter2 = outFilter2;

		n_buf_pgs = amt_of_mem;
		done = false;
		get_from_outer = true;

		AttrType[] Jtypes = new AttrType[n_out_flds];
		short[] t_size;

		perm_mat = proj_list;
		nOutFlds = n_out_flds;
//		try {
//			//t_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes, in1, len_in1, in1, len_in1, t1_str_sizes, t1_str_sizes,
//			//		proj_list, nOutFlds);
//		} catch (TupleUtilsException e) {
//			throw new NestedLoopException(e, "TupleUtilsException is caught by NestedLoopsJoins.java");
//		}

		try {
			rhf = new Heapfile(relationName);
			shf = new Heapfile(relationName2);

		} catch (Exception e) {
			throw new NestedLoopException(e, "Create new heapfile failed.");
		}
 }
	@Override
	public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException,
			InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException,
			LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
		// TODO Auto-generated method stub
		if(done){
			return null;
		}
		
		do{
			Sort sortR1 = null;
			Sort sortR2 = null;
			Sort sortS1 = null;
			Sort sortS2 = null;
			TupleOrder toL1 = null;
			TupleOrder toL2 = null;
			int len_sortR1 = 0;
			int len_sortR2 = 0;
			int len_sortS1 = 0;
			int len_sortS2 = 0;
			int at= OutputFilter[0].type2.attrType;
			int at2= OutputFilter2[0].type2.attrType;
			if(at != 0)
			{
				len_sortR1=4;
				len_sortS1=4;
			}
			else{
				System.err.println("1 String size needed: sort field");
				//len_sort=OutputFilter[0].operand2.integer;
				}
			
			if(at2 != 0)
			{
				len_sortR2=4;
				len_sortS2=4;
			}
			else{
				System.err.println("2 String size needed: sort field");
				//len_sort=OutputFilter[0].operand2.integer;
				}
			
			int opNum = OutputFilter[0].op.attrOperator;
			int opNum2 = OutputFilter2[0].op.attrOperator;
			toL1 = getTOL1(opNum);			
			toL2 = getTOL2(opNum2);
			sortR1 = new Sort(_in1,(short)in1_len,str1_size,outer,OutputFilter[0].operand2.symbol.offset,toL1,len_sortR1,12);
			
			Tuple sortedTupleR1 = null;
			int indexR1 = 0;
			while((sortedTupleR1 = sortR1.get_next()) != null){
				//System.out.println(sortedTuple.getIntFld(1) + " " + index);
				
				sortedTableR1.put(indexR1,sortedTupleR1.getIntFld(perm_mat[0].offset));
				sortedReverseR1.put(indexR1, sortedTupleR1.getIntFld(OutputFilter[0].operand1.symbol.offset));
				sortedTableR1ID.put(sortedTupleR1.getIntFld(perm_mat[0].offset),indexR1);
				//System.out.println(sortedTupleR1.getIntFld(3));
				indexR1++;
			}
			indexR1--;
			
			sortR1.close();
			
			sortR2 = new Sort(_in1,(short)in1_len,str1_size,outer2,OutputFilter2[0].operand2.symbol.offset,toL2,len_sortR2,12);
			
			Tuple sortedTupleR2 = null;
			int indexR2 = 0;
			
			while((sortedTupleR2 = sortR2.get_next()) != null){
				
				
				sortedTableR2.put(sortedTupleR2.getIntFld(OutputFilter2[0].operand1.symbol.offset),indexR2);
				sortedReverseR2.put(indexR2, sortedTupleR2.getIntFld(OutputFilter2[0].operand1.symbol.offset));
				sortedTableR2IDReverse.put(indexR2, sortedTupleR2.getIntFld(perm_mat[0].offset));
				//System.out.println(sortedTupleR2.getIntFld(1));
				indexR2++;
			}
			indexR2--;
			sortR2.close();
			
			sortS1 = new Sort(_in2,(short)in2_len,str2_size,inner,OutputFilter[0].operand2.symbol.offset,toL1,len_sortS1,12);
			
			Tuple sortedTupleS1 = null;
			int indexS1 = 0;
			while((sortedTupleS1 = sortS1.get_next()) != null){
				System.out.println("S1:--------");
				System.out.println(sortedTupleS1.getIntFld(OutputFilter[0].operand2.symbol.offset) + " "+indexS1);
				sortedTableS1.put(sortedTupleS1.getIntFld(OutputFilter[0].operand2.symbol.offset),indexS1);
				sortedReverseS1.put(indexS1, sortedTupleS1.getIntFld(OutputFilter[0].operand2.symbol.offset));
				sortedTableS1ID.put( sortedTupleS1.getIntFld(perm_mat[1].offset),indexS1);
				//sortedTableS1IDReverse.put(indexS1,sortedTupleS1.getIntFld(perm_mat[1].offset));
				indexS1++;
			}
			indexS1--;
			
			sortS1.close();
			
			
			sortS2 = new Sort(_in2,(short)in2_len,str2_size,inner2,OutputFilter2[0].operand2.symbol.offset,toL2,len_sortS2,12);
			
			Tuple sortedTupleS2 = null;
			int indexS2 = 0;
			while((sortedTupleS2 = sortS2.get_next()) != null){
				//System.out.println(sortedTuple.getIntFld(1) + " " + index);
				
				sortedTableS2.put(sortedTupleS2.getIntFld(OutputFilter2[0].operand2.symbol.offset),indexS2);
				sortedReverseS2.put(indexS2, sortedTupleS2.getIntFld(OutputFilter2[0].operand2.symbol.offset));
				sortedTableS2IDReverse.put(indexS2, sortedTupleS2.getIntFld(perm_mat[1].offset));
				indexS2++;
			}
			indexS2--;
			sortS2.close();
			
			//sortR1.get_next().getIntFld(OutputFilter[0].operand1.symbol.offset);
			//System.out.println("23333" + " " + indexR1 + " "+ indexR2);
			
			int[] p = new int[indexR2+1];
			int[] p2 = new int[indexS2+1];
			System.out.println("p: ");
			for(int n = 0; n <= indexR2; n++){
				//System.out.println(sortedTableR2IDReverse.get(n));
				p[n] = sortedTableR1ID.get(sortedTableR2IDReverse.get(n));
				//System.out.println(sortedTableR2IDReverse.get(n) + " " + sortedReverseR2.get(n) + " "+ sortedReverseR1.get(n));
				
				System.out.println(p[n]);				
				//System.out.println("here");
			}
			System.out.println("p': ");
			for(int n = 0; n <= indexS2; n++){
				p2[n] = sortedTableS1ID.get(sortedTableS2IDReverse.get(n));
				System.out.println(p2[n]);
			}
			
			float [] o1 = new float[indexR2+1];
			float [] o2 = new float[indexR2+1];
			
			float operator = (float) 0.5;
			
			//if((opNum == 1 && (opNum2 == 1||opNum2 == 4)) ||(opNum == 2 && (opNum2 == 2 || opNum2 == 5))||(opNum == 4 && (opNum2 == 4 || opNum2 == 1)) ||(opNum == 5 && (opNum2 == 5 || opNum2 == 2)))
//			if((opNum == 2 && opNum2 == 1) || (opNum == 2 && opNum2 == 2))
//			{
//				System.out.println("hahahahaha");
//				operator = -1;
//			}
			
			for(int n = 0; n <= indexR2; n++)
			{
				int l1 = sortedReverseR1.get(n);
				if( sortedTableS1.containsKey(l1) )
				{
					o1[n] = sortedTableS1.get(l1);
					//System.out.println(o1[n]);
				}
				else
				{
					for(int h = 0; h <= indexS2; h++)
					{
						if(toL1.tupleOrder == 0)//Ascending
						{
							if(l1 < sortedReverseS1.get(h))
							{
								o1[n] = h - operator;
								h = indexS2+1;
							}
						}
						else//Descending
						{
							if(l1 > sortedReverseS1.get(h))
							{//Todo
								o1[n] = h - operator;
								h = indexS2+1;
							}
						}
					}
					
					if(toL1.tupleOrder == 0)//Ascending
					{
						if(l1 > sortedReverseS1.get(indexS2))
						{
							o1[n] = indexS2+1;
						}
						if(l1 < sortedReverseS1.get(0))
						{
							o1[n] = 0 - operator;
						}
					}
					else//Descending
					{
						if(l1 > sortedReverseS1.get(0))
						{
							o1[n] = 0 - operator;
						}
						if(l1 < sortedReverseS1.get(indexS2))
						{
							o1[n] = indexS2+1;
						}
					}
				}
			}
			
			for(int n = 0; n <= indexR2; n++)
			{
				int l2 = sortedReverseR2.get(n);
				
				if( sortedTableS2.containsKey(l2) )
				{
					o2[n] = sortedTableS2.get(l2);
					//if(n == 0){System.out.println("o2[0]: "+" "+o2[n]);}
				}
				else
				{
					for(int h = 0; h <= indexS2; h++)
					{
						if(toL2.tupleOrder == 0)//Ascending
						{
							if(l2 < sortedReverseS2.get(h))
							{
								o2[n] = h - operator;
								h = indexS2+1;
							}
						}
						else//Descending
						{
							if(l2 > sortedReverseS2.get(h))
							{//Todo
								o2[n] = h - operator;
								h = indexS2+1;
							}
						}
					}
					
					if(toL2.tupleOrder == 0)//Ascending
					{
						if(l2 > sortedReverseS2.get(indexS2))
						{
							o2[n] = indexS2+1;
						}
						if(l2 < sortedReverseS2.get(0))
						{
							o2[n] = 0 - operator;
						}
					}
					else//Descending
					{
						if(l2 > sortedReverseS2.get(0))
						{
							o2[n] = 0 - operator;
						}
						if(l2 < sortedReverseS2.get(indexS2))
						{
							o2[n] = indexS2+1;
						}
					}
				}
			}
			
			System.out.println("o1 + o2:");
			for(int k = 0; k <= indexR2; k++)
			{
				System.out.println(o1[k] + " " + o2[k]);
			}
			
			
			
			File file  = new File("Task2c.txt");
			//file.getParentFile().mkdirs();
			
			PrintWriter printWriter = null;
			
			printWriter = new PrintWriter(file);
			
			int [] b = new int[indexS2+1];
			int eqOff = -1;
			//if((opNum == 1 && (opNum2 == 1||opNum2 == 4)) ||(opNum == 2 && (opNum2 == 2 || opNum2 == 5))||(opNum == 4 && (opNum2 == 4 || opNum2 == 1)) ||(opNum == 5 && (opNum2 == 5 || opNum2 == 2)))
			if((opNum == 4 || opNum == 5)&&(opNum2 == 4 || opNum2 == 5))
			{
				//Todo =0
				eqOff = 0;
			}
			else
			{
				eqOff = 1;
			}
			
			for(int i = 0; i<=indexR1;i++){
				if(o2[i] >= 0)
				{
				int off2 = (int)(o2[i]);
				//System.out.println(off2);
				//if(off2<0){off2=0;}
				if(off2>=b.length){off2 = b.length-1;}
				if(off2>=0 && off2<b.length){
					for(int j = 0;j<=off2;j++){
						b[p2[j]] = 1;
					}
				}
				}
				//b[0] =1;
				int off1 = (int)(o1[p[i]] + eqOff);
				//System.out.println(off1);
				if(off1<b.length){
					for(int k=off1;k<=indexS2;k++){
						//if(k == indexS2){System.out.println("-----");System.out.println}
						if(b[k]==1){
							System.out.println("["+sortedReverseR2.get(i)+ ", "+sortedReverseS1.get(k)+"]");
							printWriter.write(sortedReverseR2.get(i)+ ", "+sortedReverseS1.get(k));
							printWriter.write("\n");
						}
					}					
				}
				
			}
			
			//printWriter.write("xxxxx");
			printWriter.close();
//			{
//				b[p[k]] = 1;
//				for(int m = p[k]+eqOff; m<=index; m++)
//				{
//					if(b[m] == 1)
//					{
//						System.out.println("[" +sortedReverse.get(m)+","+ sortedReverse.get(p[k])+ "]");
//						//printWriter.write("[" +sortedReverse.get(m)+","+ sortedReverse.get(p[k])+ "]");
//						//printWriter.write("\n");
//					}
//					
//				}
//				//System.out.println(p[k]);
//				//System.out.println(b[0]+" "+b[1]+" "+b[2]+" "+b[3]+" "+b[4]+" ");
//				//System.out.println(b[0]+" "+b[1]+" "+b[2]);
//				
//			}
//			printWriter.close();
			
//		    java.util.Iterator it = (java.util.Iterator)sortedTable.entrySet().iterator();
//		    while (it.hasNext()) {
//		        Map.Entry pair = (Map.Entry)it.next();
//		        System.out.println(pair.getKey() + " = " + pair.getValue());
//		        it.remove(); // avoids a ConcurrentModificationException
//		    }


			
			
			
			
			
			return Jtuple;
		}
		while(true);
		
	}

	@Override
	public void close() throws IOException, JoinsException, SortException, IndexException {
		// TODO Auto-generated method stub
	      if (!closeFlag) {
	    		try {
	    		  outer.close();
	    		}catch (Exception e) {
	    		  throw new JoinsException(e, "IESelfJoin.java: error in closing iterator.");
	    		}
	    		closeFlag = true;
	    	      }
		
	}
	
	private TupleOrder getTOL2(int opNum){
		TupleOrder to = null;
		if(opNum == 2 || opNum == 5)
		{//Ascending order
			to = new TupleOrder(TupleOrder.Ascending);
			
		}
		else if(opNum == 1 || opNum == 4){
			to = new TupleOrder(TupleOrder.Descending);
		}
		else{// default ascending
			to = new TupleOrder(TupleOrder.Ascending);
		}
		return to;
	}
	
	private TupleOrder getTOL1(int opNum){
		TupleOrder to = null;
		if(opNum == 2 || opNum == 5)
		{//Descending order
			to = new TupleOrder(TupleOrder.Descending);
			
		}
		else if(opNum == 1 || opNum == 4){
			to = new TupleOrder(TupleOrder.Ascending);
		}
		else{// default ascending
			to = new TupleOrder(TupleOrder.Ascending);
		}
		return to;
	}

}
