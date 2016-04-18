package tests;
//originally from : joins.C

import iterator.*;
import heap.*;
import global.*;
import index.*;
import java.io.*;
import java.util.*;
import java.lang.*;
import diskmgr.*;
import bufmgr.*;
import btree.*; 
import catalog.*;

//Define the S schema
class RelationS {
	public int    int1;
	public int    int2;
	public int    int3;
	public int    int4;
	
	public RelationS (int _int1, int _int2, int _int3, int _int4) {
	  int1  = _int1;
	  int2  = _int2;
	  int3  = _int3;
	  int4  = _int4;
	}
}
//Define the R schema
class RelationR {
  public int    int1;
  public int    int2;
  public int    int3;
  public int    int4;
  
  public RelationR (int _int1, int _int2, int _int3, int _int4) {
    int1  = _int1;
    int2  = _int2;
    int3  = _int3;
    int4  = _int4;
  }
}

class JoinsDriver2 implements GlobalConst {
  
  private boolean OK = true;
  private boolean FAIL = false;
  private Vector relation_s;
  private Vector relation_r;
  /** Constructor
 * @throws FileNotFoundException 
   */
  public JoinsDriver2() throws FileNotFoundException {
	  relation_s  = new Vector();
	  relation_r  = new Vector();
	  
	  File fileR = new File("tests/R.txt");
	  Scanner inputR = new Scanner(fileR);
	  int ir = 0;
	  while(inputR.hasNext()) {
		  String[] intAttr = inputR.nextLine().split(",");
		  if(ir != 0) {
			  relation_r.addElement(new RelationR(Integer.parseInt(intAttr[0]), Integer.parseInt(intAttr[1]), Integer.parseInt(intAttr[2]), Integer.parseInt(intAttr[3])));
//			  for(String s : intAttr) {
//		    	  System.out.println(s);
//		      }
		  }
		  ir++;
	  }
	  inputR.close();
	  
	  File fileS = new File("tests/S.txt");
	  Scanner inputS = new Scanner(fileS);
	  int js = 0;
	  while(inputS.hasNext()) {
		  String[] intAttr = inputS.nextLine().split(",");
		  if(js != 0) {
			  relation_s.addElement(new RelationS(Integer.parseInt(intAttr[0]), Integer.parseInt(intAttr[1]), Integer.parseInt(intAttr[2]), Integer.parseInt(intAttr[3])));
//			  for(String s : intAttr) {
//		    	  System.out.println(s);
//		      }
		  }
		  js++;
	  }
	  inputS.close();
    
    boolean status = OK;
    int numRelationS = js - 1;
    int numRelationS_attrs = 4;
    int numRelationR = ir - 1;
    int numRelationR_attrs = 4;
    System.out.println(relation_r.size());
    System.out.println(relation_s.size());
//    int numboats = 5;
//    int numboats_attrs = 3;
    
    String dbpath = "/tmp/"+System.getProperty("user.name")+".minibase.jointestdb"; 
    String logpath = "/tmp/"+System.getProperty("user.name")+".joinlog";

    String remove_cmd = "/bin/rm -rf ";
    String remove_logcmd = remove_cmd + logpath;
    String remove_dbcmd = remove_cmd + dbpath;
    String remove_joincmd = remove_cmd + dbpath;

    try {
      Runtime.getRuntime().exec(remove_logcmd);
      Runtime.getRuntime().exec(remove_dbcmd);
      Runtime.getRuntime().exec(remove_joincmd);
    }
    catch (IOException e) {
      System.err.println (""+e);
    }

   
    /*
    ExtendedSystemDefs extSysDef = 
      new ExtendedSystemDefs( "/tmp/minibase.jointestdb", "/tmp/joinlog",
			      1000,500,200,"Clock");
    */

    SystemDefs sysdef = new SystemDefs( dbpath, 1000, NUMBUF, "Clock" );
    
    // creating the Relation S relation
    AttrType [] Stypes = new AttrType[4];
    Stypes[0] = new AttrType (AttrType.attrInteger);
    Stypes[1] = new AttrType (AttrType.attrInteger);
    Stypes[2] = new AttrType (AttrType.attrInteger);
    Stypes[3] = new AttrType (AttrType.attrInteger);

    //SOS
//    short [] Ssizes = new short [1];
//    Ssizes[0] = 30; //first elt. is 30
    
    Tuple t = new Tuple();
    try {
    	// Ssize = null
      t.setHdr((short) 4,Stypes, null);
    }
    catch (Exception e) {
      System.err.println("*** error in Tuple.setHdr() ***");
      status = FAIL;
      e.printStackTrace();
    }
    
    int size = t.size();
    
    // inserting the tuple into file "sailors"
    RID             rid;
    Heapfile        f = null;
    try {
      f = new Heapfile("S.in");
    }
    catch (Exception e) {
      System.err.println("*** error in Heapfile constructor ***");
      status = FAIL;
      e.printStackTrace();
    }
    
    t = new Tuple(size);
    try {
    	//Ssize = null
      t.setHdr((short) 4, Stypes, null);
    }
    catch (Exception e) {
      System.err.println("*** error in Tuple.setHdr() ***");
      status = FAIL;
      e.printStackTrace();
    }
    
    for (int i=0; i<numRelationS; i++) {
      try {
	t.setIntFld(1, ((RelationS)relation_s.elementAt(i)).int1);
	t.setIntFld(2, ((RelationS)relation_s.elementAt(i)).int2);
	t.setIntFld(3, ((RelationS)relation_s.elementAt(i)).int3);
	t.setIntFld(4, ((RelationS)relation_s.elementAt(i)).int4);
      }
      catch (Exception e) {
	System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
	status = FAIL;
	e.printStackTrace();
      }
      
      try {
	rid = f.insertRecord(t.returnTupleByteArray());
      }
      catch (Exception e) {
	System.err.println("*** error in Heapfile.insertRecord() ***");
	status = FAIL;
	e.printStackTrace();
      }      
    }
    if (status != OK) {
      //bail out
      System.err.println ("*** Error creating relation for sailors");
      Runtime.getRuntime().exit(1);
    }
    
    //creating the Relation R relation
    AttrType [] Rtype = {
      new AttrType(AttrType.attrInteger), 
      new AttrType(AttrType.attrInteger), 
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrInteger)
    };
    
//    short  []  Bsizes = new short[2];
//    Bsizes[0] = 30;
//    Bsizes[1] = 20;
    t = new Tuple();
    try {
    	//Bsizes = null
      t.setHdr((short) 4,Rtype, null);
    }
    catch (Exception e) {
      System.err.println("*** error in Tuple.setHdr() ***");
      status = FAIL;
      e.printStackTrace();
    }
    
    size = t.size();
    
    // inserting the tuple into file R
    //RID             rid;
    f = null;
    try {
      f = new Heapfile("R.in");
    }
    catch (Exception e) {
      System.err.println("*** error in Heapfile constructor ***");
      status = FAIL;
      e.printStackTrace();
    }
    
    t = new Tuple(size);
    try {
    	//Bsizes = null
      t.setHdr((short) 4, Rtype, null);
    }
    catch (Exception e) {
      System.err.println("*** error in Tuple.setHdr() ***");
      status = FAIL;
      e.printStackTrace();
    }
    
    for (int i=0; i<numRelationR; i++) {
      try {
	t.setIntFld(1, ((RelationR)relation_r.elementAt(i)).int1);
	t.setIntFld(2, ((RelationR)relation_r.elementAt(i)).int2);
	t.setIntFld(3, ((RelationR)relation_r.elementAt(i)).int3);
	t.setIntFld(4, ((RelationR)relation_r.elementAt(i)).int4);
      }
      catch (Exception e) {
	System.err.println("*** error in Tuple.setStrFld() ***");
	status = FAIL;
	e.printStackTrace();
      }
      
      try {
	rid = f.insertRecord(t.returnTupleByteArray());
      }
      catch (Exception e) {
	System.err.println("*** error in Heapfile.insertRecord() ***");
	status = FAIL;
	e.printStackTrace();
      }      
    }
    if (status != OK) {
      //bail out
      System.err.println ("*** Error creating relation for boats");
      Runtime.getRuntime().exit(1);
    }
    
    //creating the boats relation
//    AttrType [] Rtypes = new AttrType[3];
//    Rtypes[0] = new AttrType (AttrType.attrInteger);
//    Rtypes[1] = new AttrType (AttrType.attrInteger);
//    Rtypes[2] = new AttrType (AttrType.attrString);
//
//    short [] Rsizes = new short [1];
//    Rsizes[0] = 15; 
//    t = new Tuple();
//    try {
//      t.setHdr((short) 3,Rtypes, Rsizes);
//    }
//    catch (Exception e) {
//      System.err.println("*** error in Tuple.setHdr() ***");
//      status = FAIL;
//      e.printStackTrace();
//    }
//    
//    size = t.size();
//    
//    // inserting the tuple into file "boats"
//    //RID             rid;
//    f = null;
//    try {
//      f = new Heapfile("reserves.in");
//    }
//    catch (Exception e) {
//      System.err.println("*** error in Heapfile constructor ***");
//      status = FAIL;
//      e.printStackTrace();
//    }
//    
//    t = new Tuple(size);
//    try {
//      t.setHdr((short) 3, Rtypes, Rsizes);
//    }
//    catch (Exception e) {
//      System.err.println("*** error in Tuple.setHdr() ***");
//      status = FAIL;
//      e.printStackTrace();
//    }
//    
//    for (int i=0; i<numreserves; i++) {
//      try {
//	t.setIntFld(1, ((Reserves)reserves.elementAt(i)).sid);
//	t.setIntFld(2, ((Reserves)reserves.elementAt(i)).bid);
//	t.setStrFld(3, ((Reserves)reserves.elementAt(i)).date);
//
//      }
//      catch (Exception e) {
//	System.err.println("*** error in Tuple.setStrFld() ***");
//	status = FAIL;
//	e.printStackTrace();
//      }      
//      
//      try {
//	rid = f.insertRecord(t.returnTupleByteArray());
//      }
//      catch (Exception e) {
//	System.err.println("*** error in Heapfile.insertRecord() ***");
//	status = FAIL;
//	e.printStackTrace();
//      }      
//    }
//    if (status != OK) {
//      //bail out
//      System.err.println ("*** Error creating relation for reserves");
//      Runtime.getRuntime().exit(1);
//    }
    
  }
  
  public boolean runTests() {
    Disclaimer();
    Query2();
    System.out.print ("Finished joins testing"+"\n");
    return true;
  }


  private void Query2_CondExpr(CondExpr[] expr) {

    expr[0].next  = null;
    expr[0].op    = new AttrOperator(AttrOperator.aopLT);
    expr[0].type1 = new AttrType(AttrType.attrSymbol);
    expr[0].type2 = new AttrType(AttrType.attrSymbol);
    //use column 3 to join
    expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),2);
    expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),3);
    
    expr[1] = null;
  }

  
  
  public void Query2() {
    System.out.print("**********************Query2 strating *********************\n");
    boolean status = OK;

    // Sailors, Boats, Reserves Queries.
    System.out.print 
      ("Query: Find the names of sailors who have reserved "
       + "a red boat\n"
       + "       and return them in alphabetical order.\n\n"
       + "  SELECT   S.sname\n"
       + "  FROM     Sailors S, Boats B, Reserves R\n"
       + "  WHERE    S.sid = R.sid AND R.bid = B.bid AND B.color = 'red'\n"
       + "  ORDER BY S.sname\n"
       + "Plan used:\n"
       + " Sort (Pi(sname) (Sigma(B.color='red')  "
       + "|><|  Pi(sname, bid) (S  |><|  R)))\n\n"
       + "(Tests File scan, Index scan ,Projection,  index selection,\n "
       + "sort and simple nested-loop join.)\n\n");
    
    // Build Index first
    IndexType b_index = new IndexType (IndexType.B_Index);

   
    //ExtendedSystemDefs.MINIBASE_CATALOGPTR.addIndex("sailors.in", "sid", b_index, 1);
    // }
    //catch (Exception e) {
    // e.printStackTrace();
    // System.err.print ("Failure to add index.\n");
      //  Runtime.getRuntime().exit(1);
    // }
    
    


    CondExpr [] outFilter  = new CondExpr[2];
    outFilter[0] = new CondExpr();
    outFilter[1] = new CondExpr();

    Query2_CondExpr(outFilter);
    Tuple t = new Tuple();
    t = null;

 // creating the Relation S relation
    AttrType [] Stypes = new AttrType[4];
    Stypes[0] = new AttrType (AttrType.attrInteger);
    Stypes[1] = new AttrType (AttrType.attrInteger);
    Stypes[2] = new AttrType (AttrType.attrInteger);
    Stypes[3] = new AttrType (AttrType.attrInteger);
    
    //attr 1 and 3
    AttrType [] Stypes2 = {
      new AttrType(AttrType.attrInteger), 
      new AttrType(AttrType.attrInteger), 
    };

//    short []   Ssizes = new short[1];
//    Ssizes[0] = 30;
    
 // creating the Relation R relation
    AttrType [] Rtypes = new AttrType[4];
    Rtypes[0] = new AttrType (AttrType.attrInteger);
    Rtypes[1] = new AttrType (AttrType.attrInteger);
    Rtypes[2] = new AttrType (AttrType.attrInteger);
    Rtypes[3] = new AttrType (AttrType.attrInteger);

//    short  []  Rsizes = new short[1] ;
//    Rsizes[0] = 15;
    
  //R outter attr 1 and 3
    AttrType [] Rtypes2 = {
      new AttrType(AttrType.attrInteger), 
      new AttrType(AttrType.attrInteger), 
    };
    
    AttrType [] Jtypes = {
      new AttrType(AttrType.attrInteger), 
      new AttrType(AttrType.attrInteger), 
    };

//    short  []  Jsizes = new short[1];
//    Jsizes[0] = 30;
    
    FldSpec []  proj1 = {
       new FldSpec(new RelSpec(RelSpec.outer), 1),
       new FldSpec(new RelSpec(RelSpec.innerRel), 1)
    }; // R.3, S.3

    FldSpec [] Rprojection = {
       new FldSpec(new RelSpec(RelSpec.outer), 1),
       new FldSpec(new RelSpec(RelSpec.outer), 3),
       // new FldSpec(new RelSpec(RelSpec.outer), 3),
       // new FldSpec(new RelSpec(RelSpec.outer), 4)
    };
    
    FldSpec [] Sprojection = {
       new FldSpec(new RelSpec(RelSpec.innerRel), 1),
       new FldSpec(new RelSpec(RelSpec.innerRel), 3),
       // new FldSpec(new RelSpec(RelSpec.outer), 3),
       // new FldSpec(new RelSpec(RelSpec.outer), 4)
    };
 
//    CondExpr [] selects = new CondExpr[1];
//    selects[0] = null;
    
    
    //IndexType b_index = new IndexType(IndexType.B_Index);
    
   

    //_______________________________________________________________
    //*******************create an scan on the heapfile**************
    //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // create a tuple of appropriate size
    //Relation R
    Tuple tt = new Tuple();
    try {
    	//Ssizes = null
      tt.setHdr((short) 4, Rtypes, null);
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }

    int sizett = tt.size();
    tt = new Tuple(sizett);
    try {
    	//Ssizes = null
      tt.setHdr((short) 4, Rtypes, null);
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    Heapfile        f = null;
    //create Relation R heapfile 
    try {
      f = new Heapfile("R.in");
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    
    Scan scan = null;
    
    try {
      scan = new Scan(f);
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
      Runtime.getRuntime().exit(1);
    }

    // create the index file
    BTreeFile btf = null;
    try {
      btf = new BTreeFile("BTreeIndexR", AttrType.attrInteger, 4, 1); 
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
      Runtime.getRuntime().exit(1);
    }
    
    RID rid = new RID();
    int key =0;
    Tuple temp = null;
    
    try {
      temp = scan.getNext(rid);
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    while ( temp != null) {
      tt.tupleCopy(temp);
      
      try {
	key = tt.getIntFld(3);
      }
      catch (Exception e) {
	status = FAIL;
	e.printStackTrace();
      }
      
      try {
	btf.insert(new IntegerKey(key), rid); 
      }
      catch (Exception e) {
	status = FAIL;
	e.printStackTrace();
      }

      try {
	temp = scan.getNext(rid);
      }
      catch (Exception e) {
	status = FAIL;
	e.printStackTrace();
      }
    }
    
    // close the file scan
    scan.closescan();
    
  //_______________________________________________________________
    //*******************create an scan on the heapfile**************
    //+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // create a tuple of appropriate size
    //Relation S
    Tuple ttS = new Tuple();
    try {
    	//Ssizes = null
    	ttS.setHdr((short) 4, Stypes, null);
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }

    int sizettS = ttS.size();
    ttS = new Tuple(sizettS);
    try {
    	//Ssizes = null
      ttS.setHdr((short) 4, Stypes, null);
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    Heapfile        fS = null;
    //create Relation S heapfile 
    try {
      fS = new Heapfile("S.in");
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    
    Scan scanS = null;
    
    try {
      scanS = new Scan(fS);
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
      Runtime.getRuntime().exit(1);
    }

    // create the index file
    BTreeFile btfS = null;
    try {
      btfS = new BTreeFile("BTreeIndexS", AttrType.attrInteger, 4, 1); 
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
      Runtime.getRuntime().exit(1);
    }
    
    RID ridS = new RID();
    int keyS =0;
    Tuple tempS = null;
    
    try {
      tempS = scanS.getNext(ridS);
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    while ( tempS != null) {
      ttS.tupleCopy(tempS);
      
      try {
	keyS = ttS.getIntFld(3);
      }
      catch (Exception e) {
	status = FAIL;
	e.printStackTrace();
      }
      
      try {
	btfS.insert(new IntegerKey(keyS), ridS); 
      }
      catch (Exception e) {
	status = FAIL;
	e.printStackTrace();
      }

      try {
	tempS = scanS.getNext(ridS);
      }
      catch (Exception e) {
	status = FAIL;
	e.printStackTrace();
      }
    }
    
    // close the file scan
    scanS.closescan();
    
    
    //_______________________________________________________________
    //*******************close an scan on the heapfile**************
    //++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    iterator.Iterator amR = null;
    iterator.Iterator amS = null;
    System.out.print ("After Building btree index on R.3.\n\n");
    try {
      amR = new IndexScan ( b_index, "R.in",
			   "BTreeIndexR", Rtypes, null, 4, 2,
			   Rprojection, null, 1, false);
    }
    catch (Exception e) {
        System.err.println ("*** Error creating scan for Index scan");
        System.err.println (""+e);
        Runtime.getRuntime().exit(1);
      }
    
    System.out.print ("After Building btree index on S.3.\n\n");
    try {
      amS = new IndexScan ( b_index, "S.in",
			   "BTreeIndexS", Stypes, null, 4, 2,
			   Sprojection, null, 1, false);
    }
    
    catch (Exception e) {
      System.err.println ("*** Error creating scan for Index scan");
      System.err.println (""+e);
      Runtime.getRuntime().exit(1);
    }
   
    
    NestedLoopsJoins nlj = null;
    try {
      nlj = new NestedLoopsJoins (Rtypes2, 2, null,
				  Stypes, 4, null,
				  10,
				  amR, "S.in",
				  outFilter, null, proj1, 2);
    }
    catch (Exception e) {
      System.err.println ("*** Error preparing for nested_loop_join");
      System.err.println (""+e);
      e.printStackTrace();
      Runtime.getRuntime().exit(1);
    }
    
    TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
   
    t = null;
    try {
      while ((t = nlj.get_next()) != null) {
        t.print(Jtypes);
      }
    	

    }
    catch (Exception e) {
      //System.err.println (""+e);
      e.printStackTrace();
      Runtime.getRuntime().exit(1);
    }

    System.out.println ("\n"); 
    
    if (status != OK) {
      //bail out
   
      Runtime.getRuntime().exit(1);
      }
  }

  private void Disclaimer() {
    System.out.print ("\n\nAny resemblance of persons in this database to"
         + " people living or dead\nis purely coincidental. The contents of "
         + "this database do not reflect\nthe views of the University,"
         + " the Computer  Sciences Department or the\n"
         + "developers...\n\n");
  }
}

public class JoinTest2
{
  public static void main(String argv[]) throws FileNotFoundException
  {
    boolean sortstatus;
    //SystemDefs global = new SystemDefs("bingjiedb", 100, 70, null);
    //JavabaseDB.openDB("/tmp/nwangdb", 5000);

    JoinsDriver2 jjoin = new JoinsDriver2();

    sortstatus = jjoin.runTests();
    if (sortstatus != true) {
      System.out.println("Error ocurred during join tests");
    }
    else {
      System.out.println("join tests completed successfully");
    }
  }
}

