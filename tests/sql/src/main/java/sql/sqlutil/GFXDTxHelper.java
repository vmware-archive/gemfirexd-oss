/*
 * Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package sql.sqlutil;

import hydra.Log;
import hydra.TestConfig;

import java.util.ArrayList;
import java.util.HashMap;

import sql.SQLBB;
import sql.SQLPrms;
import sql.SQLTest;
import sql.dmlTxStatements.DMLTxStmtIF;

public class GFXDTxHelper {
	
  private static ArrayList<Integer>[] tidListArray;
  private static HashMap<Integer, Integer> tidListMap;
  private static String partitionByList;
  
  private static ArrayList<int []> cidRangeList; //int[] for low/upper boundary
  private static String partitionByRange;
  
  //only the ddl thread should create tables & set the info up
  public static String getTidListPartition() {
  	if (partitionByList == null) 
  		setTidListPartition();
  	return partitionByList;
  }
  
  //only the ddl thread should create tables & set the info up
  public static String getCidRangePartition() {
  	if (partitionByRange == null) {
  		int totalCid = (int) TestConfig.tab().longAt(SQLPrms.totalCids, 10000);
  		setCidRangePartition(totalCid);
  	}
  	return partitionByRange;
  }
  
  private static void setTidListPartition() {  	
  	setTidListArray(SQLTest.numOfStores, SQLTest.numOfWorkers);
  	SQLBB.getBB().getSharedMap().put("tidListArray", tidListArray);  
  	setTidListMap();
  	SQLBB.getBB().getSharedMap().put("tidListMap", tidListMap);  	
  	
  	//construct the partition by list value string
  	StringBuffer str = new StringBuffer();
  	str.append('(');
  	for (int i =0; i<tidListArray.length; i++) {
  		str.append("VALUES (");
  		for (Integer tid: tidListArray[i]) {
  			str.append(tid);
  			str.append(", ");
  		}
  		str.delete(str.lastIndexOf(", "), str.length());
  		str.append("), ");
  	}
		str.delete(str.lastIndexOf(", "), str.length());
		str.append(") ");
		Log.getLogWriter().info("string is " + str.toString());
		partitionByList = str.toString();
  }
  
  //randomly set tid list
  private static void setTidListArray(int size, int numWorkers) {
  	tidListArray = new ArrayList[size];
  	ArrayList<Integer> seed = new ArrayList<Integer>();
  	for (int i=0; i<numWorkers; i++) 
  		seed.add(i); //all the avail tid
  	
  	int bucketSize = numWorkers/size; //how many tid in each list bucket
  	if (bucketSize == 0) bucketSize++;
  	
  	for (int i=0; i<size-1; i++) {
  		tidListArray[i] = new ArrayList<Integer>();
  		for (int j=0; j<bucketSize; j++) {
  			if (seed.size()>0)
  				tidListArray[i].add(seed.remove(SQLTest.random.nextInt(seed.size())));
  		}
  	}
  	
  	tidListArray[size-1] = new ArrayList<Integer>();
  	tidListArray[size-1].addAll(seed);  	  //for the last list bucket	
  }
  
  private static void setCidRangePartition(int totalCid) {  	  	
  	setCidRange(SQLTest.numOfStores, totalCid); //additional one vm for the keys that are not in the range
  	SQLBB.getBB().getSharedMap().put("cidRangeList", cidRangeList);  	
  	
  	//construct the partition by cid range string
    //partition by range (cid) ( VALUES BETWEEN 0 AND 699, VALUES BETWEEN 699 AND 1102, 
    // VALUES BETWEEN 1102 AND 1251, VALUES BETWEEN 1251 AND 1577, VALUES BETWEEN 1577 AND 1800,
    // VALUES BETWEEN 1800 AND 10000)
  	StringBuffer str = new StringBuffer();
  	str.append('(');
  	for (int[] rangeBound: cidRangeList) {
  		str.append("VALUES BETWEEN ");
  		str.append(rangeBound[0]);
  		str.append(" AND ");
  		str.append(rangeBound[1]);
  		str.append(", ");
  	}
		str.delete(str.lastIndexOf(", "), str.length());
		str.append(") ");
		Log.getLogWriter().info("string is " + str.toString());
		partitionByRange = str.toString();
  } 
  
  //set cid range
  private static void setCidRange(int size, int totalCid) { 	
  	int rangeSize = totalCid/size;
  	cidRangeList = new ArrayList<int[]> ();
  	
  	int start = 0;
  	int end = start + rangeSize;
  	
  	for (int i=0; i<size; i++) {
  		int[] cidRange = new int [2];
  		cidRange[0] = start;
  		cidRange[1] = end;  	
  		cidRangeList.add(cidRange); //add to the list
  		start = end;
  		if (i <size-2)
  			end = start + rangeSize;
  		else
  			end = totalCid; 			  		
  	}
	  	
  }
    
  //map of <tid, whichArray>
  //it could be retrieve colocated tids
  private static void setTidListMap() {
  	tidListMap = new HashMap<Integer,Integer>();
  	for (int i=0; i<tidListArray.length; i++) {
  		for (Integer tid: tidListArray[i]) {
  			tidListMap.put(tid, i);
  		}
  	}
  }

  @SuppressWarnings("unchecked")
  public static int getColocatedTid(String tableName, int tid) {
  	if (tidListMap == null)
  		tidListMap= (HashMap<Integer, Integer>)(SQLBB.getBB().getSharedMap().get("tidListMap"));
  	if (tidListArray == null)
  		tidListArray = (ArrayList<Integer>[])(SQLBB.getBB().getSharedMap().get("tidListArray"));
  	
  	int i = 0;
  	if (isReplicate(tableName)) {
  		i = tidListMap.get(SQLTest.random.nextInt(tid));
  	} else {
  		i = tidListMap.get(tid);
  	}
  	return tidListArray[i].get(SQLTest.random.nextInt(tidListArray[i].size()));
  }

  @SuppressWarnings("unchecked")
  public static int getColocatedCid(String tableName, int cid) {
  	if (isReplicate(tableName)) {
  		return SQLTest.random.nextInt
  			((int)SQLBB.getBB().getSharedCounters().read(SQLBB.tradeCustomersPrimary));
  	} //replicated table could use any cid which will be colocated without server group
  	
  	if (cidRangeList == null)
  		cidRangeList= (ArrayList<int[]>)(SQLBB.getBB().getSharedMap().get("cidRangeList"));
  	
  	int rangeSize = cidRangeList.get(0)[1]; //first element upper bound
  	
  	for (int[] rangeBound: cidRangeList) {
  		if (cid >= rangeBound[0] && cid < rangeBound[1])
  			return SQLTest.random.nextInt(rangeSize - sql.dmlStatements.
  					AbstractDMLStmt.cidRangeForTxOp) + rangeBound[0];  //randomly choose a colocated cid
  	}
  	return cidRangeList.get(cidRangeList.size()-1)[1] + SQLTest.random.nextInt(200); 
  		//return the last cid in the cid range, which will be colocated with any cid not in the list
  }
  
  public static int[] getRangeEnds(String tableName, int cid) {
  	int[] ends = new int[2];
  	if (isReplicate(tableName)) {
  		ends[0] = SQLTest.random.nextInt
  			((int)SQLBB.getBB().getSharedCounters().read(SQLBB.tradeCustomersPrimary));
  		ends[1] = ends[0] + 200;
  	} //replicated table does not have range and keys will be colocated without server group
  	
  	if (cidRangeList == null)
  		cidRangeList= (ArrayList<int[]>)(SQLBB.getBB().getSharedMap().get("cidRangeList"));
  	
  	int rangeSize = cidRangeList.get(0)[1]; //first element upper bound
  	
  	for (int[] rangeBound: cidRangeList) {
  		if (cid >= rangeBound[0] && cid < rangeBound[1]) {
  			ends[0] = rangeBound[0];
  			ends[1] = rangeBound[1];
  			break;
  		} 			
  	} //finds ends
  	if (cid > cidRangeList.get(cidRangeList.size()-1)[1] ) {
			ends[0] = cidRangeList.get(cidRangeList.size()-1)[1];
			ends[1] = ends[0] + 200;
  	} //if greater than the largest number in all ranges
  	
  	return ends;
  }
  
  public static boolean isReplicate(String tableName) {
  	if (SQLBB.getBB().getSharedMap().get(tableName+"replicate") == null) {
  		return false;
  	} else {
  		return true;
  	}
  }
  
  public static boolean isReplicate(int whichTable) {
  	DMLTxStmtIF dmlTxStmt= new DMLTxStmtsFactory().createDMLTxStmt(whichTable);
  	if (dmlTxStmt != null)
  		return isReplicate(dmlTxStmt.getMyTableName());
  	
  	else 
  		return false; //for non implemented tx table cases
  }
  
  @SuppressWarnings("unchecked")
  public static boolean isColocatedCid(String tableName, int txId, int key) {
  	if (isReplicate(tableName)) return true;
  	else {
    	if (cidRangeList == null)
    		cidRangeList= (ArrayList<int[]>)(SQLBB.getBB().getSharedMap().get("cidRangeList"));
    	
    	int txIdCid = (Integer) SQLBB.getBB().getSharedMap().
    		get(sql.dmlStatements.AbstractDMLStmt.cid_txId + txId);
    	
    	for (int[] rangeBound: cidRangeList) {
    		if (key >= rangeBound[0] && key < rangeBound[1])
    			return (txIdCid >= rangeBound[0] && txIdCid < rangeBound[1]);
    	}
    	
    	return true; //outside the range are colocated in the same bucket
  	}
  	
  }

}
