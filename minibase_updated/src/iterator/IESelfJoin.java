package iterator;

import heap.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import index.*;
import java.lang.*;
import java.util.HashMap;
import java.util.Map;


import java.io.*;

import java.io.IOException;

import bufmgr.PageNotReadException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import index.IndexException;

public class IESelfJoin extends Iterator{

	  private AttrType      _in1[];
	  private   int        in1_len;
	  private   Iterator  outer;
	  private   Iterator  outer2;
	  private   CondExpr OutputFilter[];
	  private   CondExpr OutputFilter2[];
	  private   int        n_buf_pgs;        // # of buffer pages available.
	  private   boolean        done,         // Is the join complete
	    get_from_outer;                 // if TRUE, a tuple is got from outer
	  private   Tuple     outer_tuple, inner_tuple;
	  private   Tuple     Jtuple;           // Joined tuple
	  private   FldSpec   perm_mat[];
	  private   int        nOutFlds;
	  private   Heapfile  hf;
	  private   Scan      inner;
	  private   String  RelationName;
	  private   short[] str1_size;
	  private   int comFld;
	  private HashMap<Integer,Integer> sortedTable;
	  private HashMap<Integer,Integer> sortedReverse;
	  private HashMap<Integer,Integer> sortedTable2;
	  
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
	public IESelfJoin(AttrType in1[], int len_in1, short t1_str_sizes[], int amt_of_mem,
			String relationName, CondExpr outFilter[], CondExpr outFilter2[], FldSpec proj_list[], int n_out_flds)
			throws IOException, NestedLoopException {
		
		FldSpec[] Projection = new FldSpec[len_in1];
		for (int i = 0; i < 4; i++) {
			Projection[i] = new FldSpec(new RelSpec(RelSpec.outer), i+1);
			//System.out.println(Projection[i].offset);
		}

		try {
			outer = new FileScan(relationName, in1, t1_str_sizes, (short) len_in1, len_in1, Projection, null);
			outer2 = new FileScan(relationName, in1, t1_str_sizes, (short) len_in1, len_in1, Projection, null);
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		sortedTable = new HashMap<>();
		sortedTable2 = new HashMap<>();
		sortedReverse = new HashMap<>();
		
		RelationName = relationName;
		_in1 = new AttrType[in1.length];
		System.arraycopy(in1, 0, _in1, 0, in1.length);
		in1_len = len_in1;

		// outer = am1;
		inner_tuple = new Tuple();
		Jtuple = new Tuple();
		OutputFilter = outFilter;
		OutputFilter2 = outFilter2;

		n_buf_pgs = amt_of_mem;
		inner = null;
		done = false;
		get_from_outer = true;

		perm_mat = proj_list;
		nOutFlds = n_out_flds;
//		try {
//			//t_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes, in1, len_in1, in1, len_in1, t1_str_sizes, t1_str_sizes,
//			//		proj_list, nOutFlds);
//		} catch (TupleUtilsException e) {
//			throw new NestedLoopException(e, "TupleUtilsException is caught by NestedLoopsJoins.java");
//		}

		try {
			hf = new Heapfile(relationName);

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
			Sort sort = null;
			TupleOrder to = null;
			int len_sort = 0;
			int at= OutputFilter[0].type2.attrType;
			if(at != 0){len_sort=4;}
			else{
				System.err.println("String size needed: sort field");
				//len_sort=OutputFilter[0].operand2.integer;
				}
			
			int opNum = OutputFilter[0].op.attrOperator;
			//int opNum2 = OutputFilter2[0].op.attrOperator;
			to = getTO2(opNum);			
			//to2 = getTO2(opNum2);
			sort = new Sort(_in1,(short)in1_len,str1_size,outer,OutputFilter[0].operand2.symbol.offset,to,len_sort,12);
			//sort2 = new Sort(_in1,(short)in1_len,str1_size,outer2,OutputFilter2[0].operand2.symbol.offset,to2,len_sort2,12);
			
			
			
			Tuple sortedTuple = null;
			int index = 0;
			while((sortedTuple = sort.get_next()) != null){
				//System.out.println(sortedTuple.getIntFld(1) + " " + index);
				
				//sortedTable.put(sortedTuple.getIntFld(1),index);
				sortedReverse.put(index, sortedTuple.getIntFld(1));
				//System.out.println(sortedTuple.getIntFld(4));
				index++;
			}
			index--;
			
			sort.close();
			
			
//			p[0] = 2;
//			p[1] = 1;
//			p[2] = 0;
			
			File file  = new File("ResultFold/result.txt");
			file.getParentFile().mkdirs();
			
			PrintWriter printWriter = null;
			
			printWriter = new PrintWriter(file);
			
			//int [] b = new int[index+1];
			int eqOff = -1;
			if((opNum == 4 || opNum == 5))
			{
				eqOff = 0;
			}
			else
			{
				eqOff = 1;
			}
			
			for(int i = 0; i<=index-eqOff;i++)
			{
				for(int j = i+eqOff; j <= index;j++ )
				{
					System.out.println("[" +sortedReverse.get(i)+","+ sortedReverse.get(j)+ "]");
					printWriter.write(sortedReverse.get(i)+","+ sortedReverse.get(j));
					printWriter.write("\n");
				}

			}
			
			printWriter.close();
			


			
			
			
			
			
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
	
	
	private TupleOrder getTO2(int opNum){
		TupleOrder to = null;
		if(opNum == 2 || opNum == 5)
		{//Ascending order
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
