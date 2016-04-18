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
import iterator.IESelfJoinWithTwoPredicates;
import iterator.NestedLoopsJoins;
import iterator.RelSpec;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Scanner;
import java.util.Vector;

import btree.BTreeFile;
import btree.IntegerKey;

public class JoinsDriverTask2bSolution implements GlobalConst{

	private boolean OK = true;
	private boolean FAIL = false;
	private Vector<RelationQ> relation_q;
	private AttrType[] qAttrTypes = new AttrType[4];
	private AttrType[] joinResultTypes = new AttrType[4];
	private FldSpec[] joinFields = new FldSpec[2];
	private Heapfile qHeapfile = null;
	private int qTupleSize = 1024;
	private IESelfJoinWithTwoPredicates sjtp = null;
	private CondExpr[] queryConfiguration = new CondExpr[2];
	private CondExpr[] queryConfiguration2 = new CondExpr[2];

	/**
	 * JoinsDriverTask1bSolution Constructor
	 * 
	 * @param QFilePath
	 *            Q data set file
	 * @throws FileNotFoundException
	 *             if the data set file not found.
	 */
	public JoinsDriverTask2bSolution(String QFilePath, String QueryFilePath)
			throws FileNotFoundException {
		boolean status = OK;
		// Q in Task2b.
		relation_q = new Vector<>();
	
		int iq = loadRelationQ(QFilePath, relation_q);
		// create the query configuration outFilter
		queryConfiguration[0] = new CondExpr();
		queryConfiguration2[0] = new CondExpr();
		loadQeuryExpr(QueryFilePath, queryConfiguration, queryConfiguration2);

		int rowNumRelationQ = iq - 1;

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
		

		// for relation_r:

		qTupleSize = getTupleSize((short) 4, qAttrTypes, null, status);
		Tuple qTuple = new Tuple(qTupleSize);
		try {
			// Bsizes = null
			qTuple.setHdr((short) 4, qAttrTypes, null);
		} catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			status = FAIL;
			e.printStackTrace();
		}
		// inserting the rTuple into file R

		try {
			qHeapfile = new Heapfile("Q.in");
		} catch (Exception e) {
			System.err.println("*** error in Heapfile constructor ***");
			status = FAIL;
			e.printStackTrace();
		}

		for (int i = 0; i < rowNumRelationQ; i++) {
			try {
				qTuple.setIntFld(1, ((RelationQ) relation_q.elementAt(i)).int1);
				qTuple.setIntFld(2, ((RelationQ) relation_q.elementAt(i)).int2);
				qTuple.setIntFld(3, ((RelationQ) relation_q.elementAt(i)).int3);
				qTuple.setIntFld(4, ((RelationQ) relation_q.elementAt(i)).int4);
			} catch (Exception e) {
				System.err.println("*** error in Tuple.setStrFld() ***");
				status = FAIL;
				e.printStackTrace();
			}

			try {
				qHeapfile.insertRecord(qTuple.returnTupleByteArray());
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

		QueryTask2b();
		System.out.print("Finished Task2b joins testing" + "\n");
		return true;
	}

	public void QueryTask2b() {
		
		boolean status = OK;

		// details of Task2b query.
		System.out.print("Query:2b\n");

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
		Tuple rTuple = new Tuple(qTupleSize);
		try {
			// Ssizes = null
			rTuple.setHdr((short) 4, qAttrTypes, null);
		} catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
		}

		Scan scan = null;

		try {
			scan = new Scan(qHeapfile);
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
			sjtp = new IESelfJoinWithTwoPredicates(qAttrTypes, 4, null, 10, "Q.in", queryConfiguration,
					queryConfiguration2, joinFields, 2);
			sjtp.get_next();
			
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
		.print("********************** Task2b strating *********************\n");

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
						.println("*** error when parse WHERE Task2b: can not handle non-AND in WHERE ***");
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
	

	private int loadRelationQ(String filePath, Vector<RelationQ> relation_q)
			throws FileNotFoundException {
		
		File file = new File(filePath);
		Scanner input = new Scanner(file);
		int ir = 0;
		while (input.hasNext()) {
			String[] intAttr = input.nextLine().split(",");
			if (ir != 0) {
				relation_q.addElement(new RelationQ(Integer
						.parseInt(intAttr[0]), Integer.parseInt(intAttr[1]),
						Integer.parseInt(intAttr[2]), Integer
								.parseInt(intAttr[3])));
			}else{
				// load attr types
				// define the type of attributes in relation_r.	
				qAttrTypes = new AttrType[intAttr.length];
				for(int k =0;k<intAttr.length;k++){
					if(intAttr[k].equalsIgnoreCase("attrInteger")){
						qAttrTypes[k] = new AttrType(AttrType.attrInteger);
					}else{
						System.err.println("*** error when set attributes types Q.txt: can not handle non-integer attr: "+intAttr[k]+" ***");
						Runtime.getRuntime().exit(1);
					}
				}
			}
			ir++;
		}
		input.close();
		return ir;
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

	private int getTupleSize(short numFlds, AttrType[] sAttrTypes,
			short[] strSizes, boolean status) {
		// create Tuple sTuple to set Tuple header and get the size of the Tuple
		// sTuple.
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
