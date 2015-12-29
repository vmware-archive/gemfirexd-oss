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


public class SchwabEquiJoinQueryPerfTest extends TestCase {
    
    /** Creates a new instance of SchwabEquiJoinQueryPerfTest */
    public SchwabEquiJoinQueryPerfTest(String name) {
        super(name);
    }//end of constructor1
    
    /////////////////////
    static Cache cache;    
    static Region region1;
    static Region region2;
    static Region region3;
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
    
    static int [] regionSizes = {1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000, 11000, 12000, 13000, 14000, 15000, 16000, 17000, 18000, 19000, 20000};
    
    /////////////queries ///////////

String queries[] ={    		

		"SELECT DISTINCT  q.cusip, q.uniqueQuoteType, q.dealerPortfolio, q.channelName, q.dealerCode, q.priceType, q.price, q.lowerQty, q.upperQty, q.ytm, r.minQty, r.maxQty, r.incQty FROM /Quotes1 q, /Restricted1 r WHERE q.uniqueQuoteType = r.uniqueQuoteType",
		
		"SELECT DISTINCT  q.cusip, q.uniqueQuoteType, q.dealerPortfolio, q.channelName, q.dealerCode, q.priceType, q.price, q.lowerQty, q.upperQty, q.ytm, r.minQty, r.maxQty, r.incQty FROM /Quotes1 q, /Restricted1 r WHERE q.uniqueQuoteType = r.uniqueQuoteType AND q.lowerQty < 200",
		
		"SELECT DISTINCT  q.cusip, q.uniqueQuoteType, q.dealerPortfolio, q.channelName, q.dealerCode, q.priceType, q.price, q.lowerQty, q.upperQty, q.ytm, r.minQty, r.maxQty, r.incQty FROM /Quotes1 q, /Restricted1 r WHERE q.uniqueQuoteType = r.uniqueQuoteType AND q.lowerQty > 200 AND r.maxQty > 1200",
		
		"SELECT DISTINCT  q1.getCusip(), q1.getUniqueQuoteType(), q1.getDealerPortfolio(), q1.getChannelName(), q2.dealerCode, q2.priceType, q2.price, q2.lowerQty, q2.upperQty, q2.ytm, r.minQty, r.maxQty, r.incQty FROM /Quotes1 q1, /Quotes2 q2, /Restricted1 r WHERE q1.cusip = r.cusip AND q2.uniqueQuoteType = r.uniqueQuoteType",
		
		"SELECT DISTINCT  q1.cusip, q1.uniqueQuoteType, q2.dealerPortfolio, q2.channelName, q1.dealerCode, q2.priceType, q1.price, q2.lowerQty, q1.upperQty, q1.ytm, r.minQty, r.maxQty, r.incQty FROM /Quotes1 q1, /Quotes2 q2, /Restricted1 r WHERE q2.uniqueQuoteType = r.uniqueQuoteType AND q1.cusip = r.cusip AND q1.priceType = 'priceType1'",

		"SELECT DISTINCT  q1.cusip, q1.uniqueQuoteType, q2.dealerPortfolio, q2.channelName, q1.dealerCode, q2.priceType, q1.price, q2.lowerQty, q1.upperQty, q1.ytm, r.minQty, r.maxQty, r.incQty FROM /Quotes1 q1, /Quotes2 q2, /Restricted1 r WHERE q2.uniqueQuoteType = r.uniqueQuoteType AND q1.cusip = r.cusip AND q1.priceType = 'priceType1' AND q2.priceType = 'priceType1'",
		
		"SELECT DISTINCT  q1.cusip, q1.uniqueQuoteType, q2.dealerPortfolio, q2.channelName, q1.dealerCode, q2.priceType, q1.price, q2.lowerQty, q1.upperQty, q1.ytm, r.minQty, r.maxQty, r.incQty FROM /Quotes1 q1, /Quotes2 q2, /Restricted1 r WHERE q2.uniqueQuoteType = r.uniqueQuoteType AND q1.cusip = r.cusip AND q1.priceType = 'priceType1' AND q2.priceType = 'priceType1' AND r.maxQty > 1200",
		
		//queries for order by clause
		
		"SELECT DISTINCT  q.cusip, q.uniqueQuoteType, q.dealerPortfolio, q.channelName, q.dealerCode, q.priceType, q.price, q.lowerQty, q.upperQty, q.ytm, r.minQty, r.maxQty, r.incQty FROM /Quotes1 q, /Restricted1 r WHERE q.uniqueQuoteType = r.uniqueQuoteType order by q.cusip",
		
		"SELECT DISTINCT  q.cusip, q.uniqueQuoteType, q.dealerPortfolio, q.channelName, q.dealerCode, q.priceType, q.price, q.lowerQty, q.upperQty, q.ytm, r.minQty, r.maxQty, r.incQty FROM /Quotes1 q, /Restricted1 r WHERE q.uniqueQuoteType = r.uniqueQuoteType order by q.cusip, r.minQty",
		
		"SELECT DISTINCT  q.cusip, q.uniqueQuoteType, q.dealerPortfolio, q.channelName, q.dealerCode, q.priceType, q.price, q.lowerQty, q.upperQty, q.ytm, r.minQty, r.maxQty, r.incQty FROM /Quotes1 q, /Restricted1 r WHERE q.uniqueQuoteType = r.uniqueQuoteType order by q.cusip, q.uniqueQuoteType",
		
		"SELECT DISTINCT  q.cusip, q.uniqueQuoteType, q.dealerPortfolio, q.channelName, q.dealerCode, q.priceType, q.price, q.lowerQty, q.upperQty, q.ytm, r.minQty, r.maxQty, r.incQty FROM /Quotes1 q, /Restricted1 r WHERE q.uniqueQuoteType = r.uniqueQuoteType order by q.cusip, q.uniqueQuoteType, r.minQty",
		
		"SELECT DISTINCT  q1.getCusip(), q1.getUniqueQuoteType(), q1.getDealerPortfolio(), q1.getChannelName(), q2.dealerCode, q2.priceType, q2.price, q2.lowerQty, q2.upperQty, q2.ytm, r.minQty, r.maxQty, r.incQty FROM /Quotes1 q1, /Quotes2 q2, /Restricted1 r WHERE q1.cusip = r.cusip AND q2.uniqueQuoteType = r.uniqueQuoteType order by q1.cusip",
		
		"SELECT DISTINCT  q1.getCusip(), q1.getUniqueQuoteType(), q1.getDealerPortfolio(), q1.getChannelName(), q2.dealerCode, q2.priceType, q2.price, q2.lowerQty, q2.upperQty, q2.ytm, r.minQty, r.maxQty, r.incQty FROM /Quotes1 q1, /Quotes2 q2, /Restricted1 r WHERE q1.cusip = r.cusip AND q2.uniqueQuoteType = r.uniqueQuoteType order by q1.cusip, q2.priceType",
		
		"SELECT DISTINCT  q1.getCusip(), q1.getUniqueQuoteType(), q1.getDealerPortfolio(), q1.getChannelName(), q2.dealerCode, q2.priceType, q2.price, q2.lowerQty, q2.upperQty, q2.ytm, r.minQty, r.maxQty, r.incQty FROM /Quotes1 q1, /Quotes2 q2, /Restricted1 r WHERE q1.cusip = r.cusip AND q2.uniqueQuoteType = r.uniqueQuoteType order by q1.cusip, q2.priceType, r.minQty"
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
        TestSuite suite = new TestSuite(SchwabEquiJoinQueryPerfTest.class);
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
            factory.setValueConstraint(Quote.class);            
            region1 = cache.createRegion("Quotes1", factory.create());
            region2 = cache.createRegion("Quotes2", factory.create());
            AttributesFactory factory1  = new AttributesFactory();
            factory1.setScope(Scope.DISTRIBUTED_ACK);
            factory1.setValueConstraint(Restricted.class);   
            region3 = cache.createRegion("Restricted1", factory1.create());            
            System.out.println("Regions are created");
            
        }catch(Exception e){
            e.printStackTrace();
        }
        
    }//end of createRegion
    
    public static void populateData(int noOfObjects)throws Exception {
        /*Add objects*/
        for (int i=0; i<noOfObjects; i++){            
            region1.put(new Integer(i), new Quote(i));
            region2.put(new Integer(i), new Quote(i));
            region3.put(new Integer(i), new Restricted(i));
        }
        System.out.println("Regions are populated");
        
    }//end of populateData
    
    public static void createIndex() throws Exception{
        QueryService qs = cache.getQueryService();
        
        /*
         *Indices share the following percentages:         
         */
        
        qs.createIndex("Quotes1Region-cusipIndex1", IndexType.FUNCTIONAL, "q.cusip", "/Quotes1 q");
        qs.createIndex("Quotes1Region-quoteTypeIndex", IndexType.FUNCTIONAL, "q.uniqueQuoteType", "/Quotes1 q");
        //qs.createIndex("Quotes1Region-priceTypeIndex", IndexType.FUNCTIONAL, "q.priceType", "/Quotes1 q");
        //qs.createIndex("Quotes1Region-lowerQtyIndex", IndexType.FUNCTIONAL, "q.lowerQty", "/Quotes1 q");
        
        qs.createIndex("Quotes1Region-cusipIndex2", IndexType.FUNCTIONAL, "q.cusip", "/Quotes2 q");
        qs.createIndex("Quotes2Region-quoteTypeIndex", IndexType.FUNCTIONAL, "q.uniqueQuoteType", "/Quotes2 q");
        //qs.createIndex("Quotes2Region-priceTypeIndex", IndexType.FUNCTIONAL, "q.priceType", "/Quotes2 q");
        //qs.createIndex("Quotes2Region-lowerQtyIndex", IndexType.FUNCTIONAL, "q.lowerQty", "/Quotes2 q");
        
        qs.createIndex("RestrictedRegion-cusip", IndexType.FUNCTIONAL, "r.cusip", "/Restricted1 r");
        qs.createIndex("RestrictedRegion-quoteTypeIndex", IndexType.FUNCTIONAL, "r.uniqueQuoteType", "/Restricted1 r");
        //qs.createIndex("RestrictedRegion-maxQtyIndex-1", IndexType.FUNCTIONAL, "r.maxQty", "/Restricted1 r");
        
        System.out.println("Indices are created");
        
    }//end of createIndex
    
    public static void printSummary()throws Exception {
        System.out.println("Printing summary");
        
        if(printCntr ==1 ){
        	Date date = new Date(System.currentTimeMillis());
            //file = new FileOutputStream("./SchwabQueryPerfLog-"+(date.toGMTString().substring(0, date.toGMTString().indexOf(" GMT")).replace(' ', '-'))+".txt");
        	file = new FileOutputStream("./SchwabEquiJoinQueryPerfLog-"+(date.toGMTString().substring(0, date.toGMTString().indexOf("200")+4).replace(' ', '-'))+".txt");
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
            Collection idxs = qs.getIndexes(region1);
            wr.write("Indexes on region: "+region1.getName());
            Iterator itr = idxs.iterator();
            Index idx;
            while(itr.hasNext()){
            	idx = (Index)itr.next();
            	wr.newLine();
            	wr.write(idx.getName());
            	wr.newLine();
            }
            
            wr.write("----------------------------------------------");
            idxs = qs.getIndexes(region2);            
            wr.newLine();
            wr.newLine();
            wr.write("Indexes on region: "+region2.getName());
            wr.newLine();
            itr = idxs.iterator();            
            while(itr.hasNext()){
            	idx = (Index)itr.next();
            	wr.newLine();
            	wr.write(idx.getName());
            	wr.newLine();
            }
            
            wr.write("----------------------------------------------");
            idxs = qs.getIndexes(region3);
            wr.newLine();
            wr.newLine();
            wr.write("Indexes on region: "+region3.getName());
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
        wr.write("Priting query performance details when size of regions is: "+region1.size());
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
    
    
}//end of SchwabEquiJoinQueryPerfTest
