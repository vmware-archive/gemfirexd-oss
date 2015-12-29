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
/*
 * BaseLineAndCompareQueryPerfTest.java
 *
 * Created on October 13, 2005, 11:28 AM
 */

package com.gemstone.gemfire.cache.query;

/**
 *
 * @author  prafulla
 *This test is to baseline and compare the performance figures for index usage benchmarks for Schwab related queries.
 */

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.cache.query.data.*;
import com.gemstone.gemfire.cache.query.internal.*;
import junit.framework.*;
import java.util.*;
import java.io.*;


public class SchwabQueryPerfOnItsDataTest extends TestCase {
    
    /** Creates a new instance of SchwabQueryPerfOnItsDataTest */
    public SchwabQueryPerfOnItsDataTest(String name) {
        super(name);
    }//end of constructor1
    
    /////////////////////
    static Cache cache;    
    static Region regionPsq, regionSec, regionInv;    
    static Index index;
    static DistributedSystem ds;
    static Properties props = new Properties();
    static QueryService qs;
    static Map queriesMap = new TreeMap();
    //static Map withoutIndexTimeRegion = new TreeMap();
    static Map withIndexTimeRegion = new TreeMap();
    static Map indexNameRegion = new TreeMap();
    //static Map withoutIndexResultSetSize = new TreeMap();
    static Map withIndexResultSetSize = new TreeMap();
    
    private static FileOutputStream file;
    private static BufferedWriter wr;
    private static int printCntr = 1;
    
    static final int MAX_OBJECTS = 1000;
    static final int QUERY_EXECUTED = 5;
    
    static int [] regionSizes = {1000};
    
    /////////////queries ///////////

String queries[] ={
		"SELECT   DISTINCT inv.cusip, inv.upper_qty, "
         +"inv.quote_price, inv.quote_timestamp, "
         +"inv.price_type, "
         +"inv.max_order_qty, inv.min_order_qty, "
         +"inv.inc_order_qty "
    +"FROM /sec1 sec, /inv1 inv, /psq1 psq "
   +"WHERE inv.cusip = psq.cusip "
   +"AND inv.dealer_code = psq.dealer_code "
    +"AND inv.lower_qty = psq.lower_qty "
     +"AND inv.upper_qty = psq.upper_qty "
     +"AND inv.price_type = psq.price_type "     
     +"AND inv.cusip = sec.cusip "
     //+"AND ((inv.product_group_code = 'MUNI')) "
     +"AND inv.max_order_qty >= inv.lower_qty "
     +"AND inv.max_order_qty >= inv.min_order_qty "
     +"AND inv.min_order_qty > 0 "
   // +"AND sec.security_description IS NOT NULL "
     +"AND sec.cusip_prohibited <> 'Y' "
     +"AND inv.price_type = 'ASK' "
     //+"AND inv.quote_price > 0 "
     //+"AND inv.retail_price > 0 "     		
};
    
    ////////////////////
    
    protected void setUp() throws java.lang.Exception {
    }//end of setUp
    
    protected void tearDown() throws java.lang.Exception {
    	closeCache();
    }//end of tearDown
    
    public void closeCache() throws java.lang.Exception{
    	if(! cache.isClosed() || cache != null){
    		cache.close();
    	}
    	if(ds.isConnected()){
    		ds.disconnect();
    	}
    }//end of closeCache
    
    
    public static Test suite(){
        TestSuite suite = new TestSuite(SchwabQueryPerfOnItsDataTest.class);
        return suite;
    }//end of suite
    
    ////////////////test methods ///////////////
    
    public void testPerf() throws Exception{
    	
    	for(int i=0; i < regionSizes.length; i++){
    		System.out.println("Size of region is: "+regionSizes[i]);
    	    queriesMap.clear();    	    
    	    withIndexTimeRegion.clear();
    	    indexNameRegion.clear();    	    
    	    withIndexResultSetSize.clear();
    	    
    		executeTestPerf(regionSizes[i]);
    		
    		closeCache();
    	}
    	
    }//end of testPerf
    
    
    public void executeTestPerf(int regionSize) throws Exception{
        createRegion();
        populateData(regionSize);
        
        String sqlStr;
        long startTime, endTime, totalTime = 0;
        SelectResults rs=null;
        Query q;
        
        /////without index ////////
        /*for (int x = 0; x<queries.length; x++){
            System.out.println("Query No: "+ (x+1) + "...without index execution");
            sqlStr = queries[x];
            QueryService qs = cache.getQueryService();
            q = qs.newQuery(sqlStr);
            totalTime = 0;
            
            queriesMap.put(new Integer(x), q);
            
            for (int i=0; i<QUERY_EXECUTED; i++){
                startTime = System.currentTimeMillis();
                rs = (SelectResults) q.execute();
                endTime = System.currentTimeMillis();
                totalTime = totalTime + (endTime - startTime);
            }
            
            long withoutIndexTime = totalTime/ QUERY_EXECUTED;
            
            withoutIndexTimeRegion.put(new Integer(x), new Long(withoutIndexTime));
            
            withoutIndexResultSetSize.put(new Integer(x), new Integer(rs.size()));
        }*/
        
        ////////// create index
        createIndex();
        
        ///////// measuring time with index
        for (int x = 0; x<queries.length; x++){
            System.out.println("Query No: "+ (x+1) + "...with index execution");
            sqlStr = queries[x];
            QueryService qs2 = cache.getQueryService();//????
            q = qs2.newQuery(sqlStr);
            
            queriesMap.put(new Integer(x), q);
            
            QueryObserverImpl observer = new QueryObserverImpl();
            QueryObserverHolder.setInstance(observer);
            
            totalTime = 0;
            
            for (int i=0; i<QUERY_EXECUTED; i++){
                startTime = System.currentTimeMillis();
                rs = (SelectResults) q.execute();
                endTime = System.currentTimeMillis();
                //System.out.println("---Result Set size is:---- "+rs.size());
                //System.out.println("-------"+(endTime - startTime));
                totalTime = totalTime + (endTime - startTime);
                
                if(i == 0){
                    ArrayList al = new ArrayList();
                    Iterator itr = observer.indexesUsed.iterator();
                    while(itr.hasNext()){
                        al.add(itr.next());
                    }                    
                    indexNameRegion.put(new Integer(x), al);
                }
            }//end of for loop
            
            long withIndexTime = totalTime/ QUERY_EXECUTED;
            
            withIndexTimeRegion.put(new Integer(x), new Long(withIndexTime));
            
            withIndexResultSetSize.put(new Integer(x), new Integer(rs.size()));            
        }
        
        printSummary();
        
    }//end of testPerf
    //public void testPerfWithIndex(){}//end of testPerfWithIndex
    
    ///////// supplementary methods /////////////
    public static void createRegion(){
        try{
        	/*create two regions which have Quote type of objects and create a region which has restricted typed of object*/
            ds = DistributedSystem.connect(props);
            cache = CacheFactory.create(ds);
            AttributesFactory factory  = new AttributesFactory();
            factory.setScope(Scope.DISTRIBUTED_ACK);
            factory.setValueConstraint(SecurityMaster.class);            
            regionSec = cache.createRegion("sec1", factory.create());
            
            AttributesFactory factory1  = new AttributesFactory();
            factory1.setScope(Scope.DISTRIBUTED_ACK);
            factory1.setValueConstraint(Inventory.class);            
            regionInv = cache.createRegion("inv1", factory1.create());            
            
            AttributesFactory factory2  = new AttributesFactory();
            factory2.setScope(Scope.DISTRIBUTED_ACK);
            factory2.setValueConstraint(ProhibitedSecurityQuote.class);   
            regionPsq = cache.createRegion("psq1", factory2.create());
            
            System.out.println("Regions are created");
            
        }catch(Exception e){
            e.printStackTrace();
        }
        
    }//end of createRegion
    
    public static void populateData(int noOfObjects)throws Exception {
        /*Add objects*/
    	//BufferedReader br = new BufferedReader(FileReader());
    	//BufferedReader brPsq = new BufferedReader(new FileReader("D:\\GemfirePC\\querying\\testing\\schwabdatafiles\\psq.txt"));
    	//BufferedReader brInv = new BufferedReader(new FileReader("D:\\GemfirePC\\querying\\testing\\schwabdatafiles\\inv.txt"));
    	//BufferedReader brSec = new BufferedReader(new FileReader("D:\\GemfirePC\\querying\\testing\\schwabdatafiles\\sec.txt"));    	
    	
    	BufferedReader brPsq = new BufferedReader(new FileReader(System.getProperty("JTESTS")+"/com/gemstone/gemfire/cache/query/data/psq.txt"));
    	BufferedReader brInv = new BufferedReader(new FileReader(System.getProperty("JTESTS")+"/com/gemstone/gemfire/cache/query/data/inv.txt"));
    	BufferedReader brSec = new BufferedReader(new FileReader(System.getProperty("JTESTS")+"/com/gemstone/gemfire/cache/query/data/sec.txt"));
    	
    	String line = null;
    	int i=0;
    	while((line = brPsq.readLine()) != null){    		
    		regionPsq.put(new Integer(i), new ProhibitedSecurityQuote (line));
    		i++;
    	}    	
    	
//    	line = null; (redundant assignment)
    	i=0;
    	while((line = brInv.readLine()) != null){
    		regionInv.put(new Integer(i), new Inventory (line));
    		i++;
    	}    	
    	
//    	line = null; (redundant assignment)
    	i=0;
    	while((line = brSec.readLine()) != null){    		
    		regionSec.put(new Integer(i), new SecurityMaster (line));
    		i++;
    	} 
        
        System.out.println("Regions are populated");
        
    }//end of populateData
    
    public static void createIndex() throws Exception{
        QueryService qs = cache.getQueryService();
        
        /*
         *Indices share the following percentages:         
         */
        
        qs.createIndex("inv-cusip", IndexType.FUNCTIONAL, "inv.cusip", "/inv1 inv"); 
        //qs.createIndex("inv - dealer_code", IndexType.FUNCTIONAL, "inv.dealer_code", "/inv1 inv"); 
        
        qs.createIndex("psq-cusip", IndexType.FUNCTIONAL, "psq.cusip", "/psq1 psq");
        //qs.createIndex("psq-status", IndexType.FUNCTIONAL, "psq.status", "/psq1 psq");
        //qs.createIndex("psq - dealer_code", IndexType.FUNCTIONAL, "psq.dealer_code", "/psq1 psq");
        
        qs.createIndex("sec-cusip", IndexType.FUNCTIONAL, "sec.cusip", "/sec1 sec");
        
        System.out.println("Indices are created");
        
    }//end of createIndex
    
    public static void printSummary()throws Exception {
        System.out.println("Printing summary");
        
        if(printCntr ==1 ){
        	java.util.Date date = new java.util.Date(System.currentTimeMillis());
            //file = new FileOutputStream("./SchwabQueryPerfLog-"+(date.toGMTString().substring(0, date.toGMTString().indexOf(" GMT")).replace(' ', '-'))+".txt");
        	file = new FileOutputStream("./SchwabQueryPerfLogOnItsData-"+(date.toGMTString().substring(0, date.toGMTString().indexOf("200")+4).replace(' ', '-'))+".txt");
            wr = new BufferedWriter(new OutputStreamWriter(file));
            
            wr.write("===========================================================================");
            wr.newLine();
            wr.write("====================QUERY PERFORMANCE REPORT===============================");                      
            wr.newLine();
            wr.newLine();
            wr.flush();            
            wr.write("Timings are the average of times for execution of query "+QUERY_EXECUTED+" number of times");
            wr.newLine();
            wr.write("Timings are measured in milliseconds");
            wr.newLine();
            wr.newLine();
            wr.write("There are following indexes on regions");
            wr.newLine();
            wr.newLine();
            wr.write("----------------------------------------------");
            wr.newLine();
            
            QueryService qs = cache.getQueryService();            
            Collection idxs = qs.getIndexes(regionPsq);
            wr.write("Indexes on region: "+regionPsq.getName());
            Iterator itr = idxs.iterator();
            Index idx;
            while(itr.hasNext()){
            	idx = (Index)itr.next();
            	wr.newLine();
            	wr.write(idx.getName());
            	wr.newLine();
            }
            
            wr.write("----------------------------------------------");
            idxs = qs.getIndexes(regionSec);            
            wr.newLine();
            wr.newLine();
            wr.write("Indexes on region: "+regionSec.getName());
            wr.newLine();
            itr = idxs.iterator();            
            while(itr.hasNext()){
            	idx = (Index)itr.next();
            	wr.newLine();
            	wr.write(idx.getName());
            	wr.newLine();
            }
            
            wr.write("----------------------------------------------");
            idxs = qs.getIndexes(regionInv);
            wr.newLine();
            wr.newLine();
            wr.write("Indexes on region: "+regionInv.getName());
            wr.newLine();
            itr = idxs.iterator();            
            while(itr.hasNext()){
            	idx = (Index)itr.next();
            	wr.newLine();
            	wr.write(idx.getName());
            	wr.newLine();
            }
            
            wr.write("===========================================================================");
        }
        
        wr.newLine();
        wr.newLine();
        wr.write("Priting query performance details when size of regions is: "+regionPsq.keys().size());
        wr.newLine();
        wr.newLine();
        
        
        Set set0 = queriesMap.keySet();
        Iterator itr0 = set0.iterator();
        int cnt = 1;
        Integer it;
        while(itr0.hasNext()){
            wr.write("Printing details for query no: "+cnt);
            wr.newLine();
            wr.newLine();
            it = (Integer) itr0.next();
            Query q1 = (Query) queriesMap.get(it);
            wr.write("Query string is: ");
            wr.newLine();
            wr.write(q1.getQueryString());
            wr.newLine();
            wr.newLine();
            
            //wr.write("Time taken without index is: "+withoutIndexTimeRegion.get(it));
            //wr.newLine();
            //wr.newLine();
            
            //Query q2 = (Query) itr2.next();
            wr.write("Time taken with index is: "+withIndexTimeRegion.get(it));
            wr.newLine();
            wr.newLine();
            
            //wr.write("Size of result set without index is: "+withoutIndexResultSetSize.get(it));
            //wr.newLine();
            //wr.newLine();
            wr.write("Size of result set with index is: "+withIndexResultSetSize.get(it));
            wr.newLine();
            wr.newLine();
            
            wr.write("Indices used are: ");
            wr.newLine();
            wr.newLine();            
            
            ArrayList al =  (ArrayList) indexNameRegion.get(it);
            
            if (al.size() == 0){
                wr.write("No indices are getting used in this query");
                wr.newLine();
                wr.newLine();
            }else{
                Iterator itr4 =  al.iterator();
                while(itr4.hasNext()){
                    wr.write(itr4.next().toString());
                    wr.newLine();
                }
            }
            
            printCntr++;
            cnt++;
            wr.write("===========================================================================");
            wr.newLine();
            wr.newLine();
            wr.flush();
        }
        
        wr.write("===========================================================================");
        wr.flush();
        
    }//end of printSummary
    
    ////////// main method ///////////
    public static void main(java.lang.String[] args) {
        junit.textui.TestRunner.run(suite());
    }//end of main method
    
    
    ////// query observer to get which indices are getting used /////
    class QueryObserverImpl extends QueryObserverAdapter {
        boolean isIndexesUsed = false;
        ArrayList indexesUsed = new ArrayList();
        
        public void beforeIndexLookup(Index index, int oper, Object key) {
            indexesUsed.add(index.getName());
        }//////
        
        public void afterIndexLookup(Collection results) {
            if (results != null) {
                isIndexesUsed = true;
            }/////////
        }
    }//end of QueryObserverImpls
    
    
}//end of SchwabQueryPerfOnItsDataTest
