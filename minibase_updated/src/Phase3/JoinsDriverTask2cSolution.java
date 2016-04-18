package Phase3;

import global.AttrOperator;
import global.AttrType;
import global.GlobalConst;
import global.IndexType;
import global.RID;
import global.SystemDefs;
import heap.Heapfile;
import heap.Scan;
import heap.Tuple;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.IEJoinWithTwoPredicates;
import iterator.RelSpec;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Scanner;
import java.util.Vector;

import btree.BTreeFile;
import btree.IntegerKey;

public class JoinsDriverTask2cSolution implements GlobalConst{

	private boolean OK = true;
	private boolean FAIL = false;
	private Vector<RelationS> relation_s;
	private Vector<RelationR> relation_r;
	private AttrType[] sAttrTypes = new AttrType[4];
	private AttrType[] rAttrTypes = null;
	private FldSpec[] joinFields = new FldSpec[2];
	private AttrType[] joinResultTypes = new AttrType[2];
	private Heapfile rHeapfile = null;
	private int rTupleSize = 1024;
	private int sTupleSize = 1024;
	private IEJoinWithTwoPredicates nlj = null;
	private CondExpr[] queryConfiguration = new CondExpr[2];
	private CondExpr[] queryConfiguration2 = new CondExpr[2];

	/**
	 * JoinsDriverTask2cSolution Constructor
	 * 
	 * @param RFilePath
	 *            R data set file
	 * @param SFilePath
	 *            S data set file
	 * @throws FileNotFoundException
	 *             if the data set file not found.
	 */
	public JoinsDriverTask2cSolution(String RFilePath, String SFilePath, String QueryFilePath)
			throws FileNotFoundException {
		
		boolean status = OK;
		// S and R in Task2c.
		relation_s = new Vector<>();
		relation_r = new Vector<>();

		int ir = loadRelationR(RFilePath, relation_r);
		int js = loadRelationS(SFilePath, relation_s);
		// create the query configuration outFilter
		queryConfiguration[0] = new CondExpr();
		queryConfiguration2[0] = new CondExpr();
		loadQeuryExpr(QueryFilePath, queryConfiguration, queryConfiguration2);

		int rowNumRelationS = js - 1;
		int rowNumRelationR = ir - 1;

		String dbpath = "/tmp/" + System.getProperty("user.name")
				+ ".minibase.jointestdb";
		String logpath = "/tmp/" + System.getProperty("user.name") + ".joinlog";

		String remove_cmd = "/bin/rm -rf ";
		String remove_logcmd = remove_cmd + logpath;
		String remove_dbcmd = remove_cmd + dbpath;
		String remove_joincmd = remove_cmd + dbpath;

		try {
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
			Runtime.getRuntime().exec(remove_joincmd);
		} catch (IOException e) {
			System.err.println("" + e);
		}

		/*
		 * ExtendedSystemDefs extSysDef = new ExtendedSystemDefs(
		 * "/tmp/minibase.jointestdb", "/tmp/joinlog", 1000,500,200,"Clock");
		 */

		@SuppressWarnings("unused")
		SystemDefs sysdef = new SystemDefs(dbpath, 1000, NUMBUF, "Clock");

		// for relation_s:

		// get the size of the Tuple sTuple.
		sTupleSize = getTupleSize((short) 4, sAttrTypes, null, status);
		// create Tuple sTuple with sTupleSize.
		Tuple sTuple = new Tuple(sTupleSize);
		try {
			// Ssize = null
			sTuple.setHdr((short) 4, sAttrTypes, null);
		} catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			status = FAIL;
			e.printStackTrace();
		}

		// inserting the tuple into file "S.in"
		Heapfile sHeapfile = null;
		try {
			sHeapfile = new Heapfile("S.in");
		} catch (Exception e) {
			System.err.println("*** error in Heapfile constructor ***");
			status = FAIL;
			e.printStackTrace();
		}

		for (int i = 0; i < rowNumRelationS; i++) {
			try {
				sTuple.setIntFld(1, ((RelationS) relation_s.elementAt(i)).int1);
				sTuple.setIntFld(2, ((RelationS) relation_s.elementAt(i)).int2);
				sTuple.setIntFld(3, ((RelationS) relation_s.elementAt(i)).int3);
				sTuple.setIntFld(4, ((RelationS) relation_s.elementAt(i)).int4);
			} catch (Exception e) {
				System.err
						.println("*** Heapfile error in Tuple.setStrFld() ***");
				status = FAIL;
				e.printStackTrace();
			}

			try {
				sHeapfile.insertRecord(sTuple.returnTupleByteArray());
			} catch (Exception e) {
				System.err.println("*** error in Heapfile.insertRecord() ***");
				status = FAIL;
				e.printStackTrace();
			}
		}
		if (status != OK) {
			System.err.println("*** Error creating relation for sailors");
			Runtime.getRuntime().exit(1);
		}

		// for relation_r:
		

		rTupleSize = getTupleSize((short) 4, rAttrTypes, null, status);
		Tuple rTuple = new Tuple(rTupleSize);
		try {
			// Bsizes = null
			rTuple.setHdr((short) 4, rAttrTypes, null);
		} catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			status = FAIL;
			e.printStackTrace();
		}
		// inserting the rTuple into file R

		try {
			rHeapfile = new Heapfile("R.in");
		} catch (Exception e) {
			System.err.println("*** error in Heapfile constructor ***");
			status = FAIL;
			e.printStackTrace();
		}

		for (int i = 0; i < rowNumRelationR; i++) {
			try {
				rTuple.setIntFld(1, ((RelationR) relation_r.elementAt(i)).int1);
				rTuple.setIntFld(2, ((RelationR) relation_r.elementAt(i)).int2);
				rTuple.setIntFld(3, ((RelationR) relation_r.elementAt(i)).int3);
				rTuple.setIntFld(4, ((RelationR) relation_r.elementAt(i)).int4);
			} catch (Exception e) {
				System.err.println("*** error in Tuple.setStrFld() ***");
				status = FAIL;
				e.printStackTrace();
			}

			try {
				rHeapfile.insertRecord(rTuple.returnTupleByteArray());
			} catch (Exception e) {
				System.err.println("*** error in Heapfile.insertRecord() ***");
				status = FAIL;
				e.printStackTrace();
			}
		}
		if (status != OK) {
			// bail out
			System.err.println("*** Error creating relation for boats");
			Runtime.getRuntime().exit(1);
		}

	}

	public boolean runTests() {
		QueryTask2c();
		System.out.print("Finished Task1b joins testing" + "\n");
		return true;
	}

	public void QueryTask2c() {		
		boolean status = OK;
		// Build Index first
		@SuppressWarnings("unused")
		IndexType b_index = new IndexType(IndexType.B_Index);

		// set join attributes type
		joinResultTypes[0] = new AttrType(AttrType.attrInteger);
		joinResultTypes[1] = new AttrType(AttrType.attrInteger);

		// _______________________________________________________________
		// *******************create an scan on the heapfile**************
		// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
		// create a tuple of appropriate size
		// Relation R
		Tuple rTuple = new Tuple(rTupleSize);
		try {
			// Ssizes = null
			rTuple.setHdr((short) 4, rAttrTypes, null);
		} catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
		}

		Scan scan = null;

		try {
			scan = new Scan(rHeapfile);
		} catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		// create the index file
		BTreeFile rBTreeFile = null;
		try {
			rBTreeFile = new BTreeFile("BTreeIndexR", AttrType.attrInteger, 4,
					1);
		} catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		RID rid = new RID();
		int key = 0;
		Tuple temp = null;
		try {
			temp = scan.getNext(rid);
			while (temp != null) {
				rTuple.tupleCopy(temp);
				key = rTuple.getIntFld(3);
				rBTreeFile.insert(new IntegerKey(key), rid);
				temp = scan.getNext(rid);
			}
		} catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
		}

		// close the file scan
		scan.closescan();

		try {
			nlj = new IEJoinWithTwoPredicates(rAttrTypes, 4, null, "R.in",sAttrTypes,
					4, null, 10,"S.in", queryConfiguration,queryConfiguration2,
					joinFields, 2);
			nlj.get_next();
//			System.out.println("#:"+nljCopy.get_next());
//			System.out.println("2#:"+nlj.get_next());
		} catch (Exception e) {
			System.err.println("*** Error preparing for nested_loop_join");
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
		
		if (status != OK) {
			Runtime.getRuntime().exit(1);
		}
	}

	private void loadQeuryExpr(String filePath, CondExpr[] expr, CondExpr[] expr2)
			throws FileNotFoundException {
		System.out
		.print("********************** Task2c strating *********************\n");

		expr[0].next = null;
		expr[0].type1 = new AttrType(AttrType.attrSymbol);
		expr[0].type2 = new AttrType(AttrType.attrSymbol);
		
		expr2[0].next = null;		
		expr2[0].type1 = new AttrType(AttrType.attrSymbol);
		expr2[0].type2 = new AttrType(AttrType.attrSymbol);

		File file = new File(filePath);
		Scanner input = new Scanner(file);

		// SELECT
		if (input.hasNext()) {
			String select = input.nextLine();
			System.out.println(select);
			String[] eachSelect = select.split(" ");
			String eachTableandFiled = eachSelect[0].split("_")[1];
			String eachTableandFiled2 = eachSelect[1].split("_")[1];
			joinFields[0] = new FldSpec(new RelSpec(RelSpec.outer),
					Integer.parseInt(eachTableandFiled));
			joinFields[1] = new FldSpec(new RelSpec(RelSpec.innerRel),
					Integer.parseInt(eachTableandFiled2));
		}else{
			System.err.println("*** error: no SELECT in query. ***");
			Runtime.getRuntime().exit(1);
		}
		// FROM
		if (input.hasNext()) {
			String from = input.nextLine();
			System.out.println(from);
		}else{
			System.err.println("*** error: no FROM in query. ***");
			Runtime.getRuntime().exit(1);
		}
		// WHERE
		if (input.hasNext()) {
			String where1 = input.nextLine();
			System.out.println(where1);
			String[] eachWhere = where1.split(" ");
			String eachWhereFiled = eachWhere[0].split("_")[1];
			String eachWhereFiled2 = eachWhere[2].split("_")[1];
			expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), Integer.parseInt(eachWhereFiled));
			expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), Integer.parseInt(eachWhereFiled2));
			int op1 = Integer.parseInt(eachWhere[1]);
			expr[0].op = new AttrOperator(mapOpNum(op1));
		}else{
			System.err.println("*** error: no WHERE in query. ***");
			Runtime.getRuntime().exit(1);
		}
		// AND
		if (input.hasNext()) {
			String and = input.nextLine();
			System.out.println(and);
			if (!and.equalsIgnoreCase("AND")) {
				System.err
						.println("*** error when parse WHERE Task2c: can not handle non-AND in WHERE ***");
				Runtime.getRuntime().exit(1);
			}
		}else{
			System.err.println("*** error: no AND in query. ***");
			Runtime.getRuntime().exit(1);
		}	
		// WHERE
		if(input.hasNext()){
			String where2 = input.nextLine();
			System.out.println(where2);
			String[] eachWhere2 = where2.split(" ");
			String eachWhereFiled1 = eachWhere2[0].split("_")[1];
			String eachWhereFiled22 = eachWhere2[2].split("_")[1];
			expr2[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), Integer.parseInt(eachWhereFiled1));
			expr2[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), Integer.parseInt(eachWhereFiled22));
			int op2 = Integer.parseInt(eachWhere2[1]);
			expr2[0].op = new AttrOperator(mapOpNum(op2));
		}else{
			System.err.println("*** error: no second WHERE in query. ***");
			Runtime.getRuntime().exit(1);
		}	

		input.close();
	}
	
	private static int mapOpNum(int queryOpNum) {
		if (queryOpNum == 2) {
			return 4;
		} else if (queryOpNum == 3) {
			return 5;
		} else if (queryOpNum == 4) {
			return 2;
		} else
			return queryOpNum;
	}

	private int loadRelationR(String filePath, Vector<RelationR> relation_r) throws FileNotFoundException {
		
		File file = new File(filePath);
		Scanner input = new Scanner(file);
		int ir = 0;
		while (input.hasNext()) {
			String[] intAttr = input.nextLine().split(",");
			if (ir != 0) {
				// load data
				relation_r.addElement(new RelationR(Integer
						.parseInt(intAttr[0]), Integer.parseInt(intAttr[1]),
						Integer.parseInt(intAttr[2]), Integer
								.parseInt(intAttr[3])));
			}else{
				// load attr types
				// define the type of attributes in relation_r.	
				rAttrTypes = new AttrType[intAttr.length];
				for(int k =0;k<intAttr.length;k++){
					if(intAttr[k].equalsIgnoreCase("attrInteger")){
						rAttrTypes[k] = new AttrType(AttrType.attrInteger);
					}else{
						System.err.println("*** error when set attributes types R.txt: can not handle non-integer attr: "+intAttr[k]+" ***");
						Runtime.getRuntime().exit(1);
					}
				}
			}
			ir++;
		}
		input.close();
		return ir;
	}

	private int loadRelationS(String filePath, Vector<RelationS> relation_s)
			throws FileNotFoundException {
		File fileS = new File(filePath);
		Scanner inputS = new Scanner(fileS);
		int js = 0;
		while (inputS.hasNext()) {
			String[] intAttr = inputS.nextLine().split(",");
			if (js != 0) {
				relation_s.addElement(new RelationS(Integer
						.parseInt(intAttr[0]), Integer.parseInt(intAttr[1]),
						Integer.parseInt(intAttr[2]), Integer
								.parseInt(intAttr[3])));
			}else{
				// load attr types
				// define the type of attributes in relation_s.	
				sAttrTypes = new AttrType[intAttr.length];
				for(int k =0;k<intAttr.length;k++){
					if(intAttr[k].equalsIgnoreCase("attrInteger")){
						sAttrTypes[k] = new AttrType(AttrType.attrInteger);
					}else{
						System.err.println("*** error when set attributes types S.txt: can not handle non-integer attr: "+intAttr[k]+" ***");
						Runtime.getRuntime().exit(1);
					}
				}
			}
			js++;
		}
		inputS.close();
		return js;
	}

	private int getTupleSize(short numFlds, AttrType[] sAttrTypes,
			short[] strSizes, boolean status) {
		// create Tuple sTuple to set Tuple header and get the size of the Tuple
		Tuple sTuple = new Tuple();
		try {
			// setHdr(short numFlds, AttrType[] types, short[] strSizes) and the
			// strSizes = null in relation_s.
			sTuple.setHdr((short) 4, sAttrTypes, null);
		} catch (Exception e) {
			System.err.println("*** error in sTuple.setHdr() ***");
			status = FAIL;
			e.printStackTrace();
		}
		return sTuple.size();
	}
}
