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
package sustainability;

import hydra.ClientVmInfo;
import hydra.ClientVmMgr;
import hydra.ClientVmNotFoundException;
import hydra.Log;
import hydra.MasterController;
import hydra.TestConfig;
import hydra.blackboard.SharedMap;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;
import java.util.Set;
import java.util.Iterator;
import java.util.Vector;

import roles.RolesPrms;

import util.CacheUtil;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheExistsException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.LossAction;
import com.gemstone.gemfire.cache.MembershipAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionExistsException;
import com.gemstone.gemfire.cache.ResumptionAction;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.TimeoutException;
import com.gemstone.gemfire.internal.cache.CachePerfStats;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXmlGenerator;
import java.util.HashMap;

public class SustainabilityTest {
	/**
	 * String representing a region name.
	 * */
	final static String rr1 = "rolePlayer";
	
	/**
	 * String representing a role.
	 * */
    final static String rr2 = "RoleB";
    /**
     * String array representing required role for a region ReliablityAttribute.
     * */
    final static String[] requiredRoles = { rr1 };
    /**
     * String representing a Region name.
     * */
    final static String regionName = "MyRegion";
    /**
     * String representing a Region name.
     * */
    final static String regionNameA = "regionA";
    /**
     * String representing a Region name.
     * */
    final static String regionNameB = "regionB";
    /**
     * String representing a Region name.
     * */
    final static String regionNameC = "regionC";
    /**
     * String representing a Region name.
     * */
    final static String regionNameD = "regionD";
    /**
     * String representing a Region name.
     * */
    final static String regionNameE = "regionE";
    /**
     * String representing a Region name.
     * */
    final static String regionNameF = "regionF";
    /**
     * String representing a Region name.
     * */
    final static String regionNameG = "regionG";
    /**
     * String representing a cahce xml file name being generated and used during 
     * reconnect.
     * */
    final static String xmlFileName = "RoleReconnect-cache.xml";
    /**
     * String representing a key of a key value pair being used to populate
     * the regions.
     * */
    final static String key = "MyKey";
    /**
     * String representing a value of a key value pair being used to populate
     * the regions.
     * */
    final static String value = "MyValue";
    /**
     * String representing a role.
     * */
    final static String roleA = "rolePlayerA";
    /**
     * String representing a role.
     * */
    final static String roleB = "rolePlayerB";
    /**
     * String representing a role.
     * */
    final static String roleC = "rolePlayerC";
    /**
     * String representing a role.
     * */
    final static String roleD = "rolePlayerD";
    /**
     * String representing a role.
     * */
    final static String roleE = "rolePlayerE";
    /**
     * String representing a role.
     * */
    final static String roleF = "rolePlayerF";
    /**
     * String representing a role.
     * */
    final static String roleG = "rolePlayerG"; 
    /**
     * String array representing a set of roleplayers needed by a particular 
     * region regionA.
     * */
    final static String[] roleRequiredRegionA = {roleA};
    /**
     * String array representing a set of roleplayers needed by a particular 
     * region regionB.
     * */
    final static String[] roleRequiredRegionB = {roleB};
    /**
     * String array representing a set of roleplayers needed by a particular 
     * region regionC.
     * */
    final static String[] roleRequiredRegionC = {roleC};
    /**
     * String array representing a set of roleplayers needed by a particular 
     * region regionD.
     * */
    final static String[] roleRequiredRegionD = {roleD};
    /**
     * String array representing a set of roleplayers needed by a particular 
     * region regionE.
     * */
    final static String[] roleRequiredRegionE = {roleE};
    /**
     * String array representing a set of roleplayers needed by a particular 
     * region regionF.
     * */
    final static String[] roleRequiredRegionF = {roleF};
    /**
     * String array representing a set of roleplayers needed by a particular 
     * region regionG.
     * */
    final static String[] roleRequiredRegionG = {roleG};
    /**
     * String representing an xml file name.
     * */
    final static String xmlFileNameRequireAB = "RequiresRoleAB-cache.xml";
    
    //final static HashMap region2HashMap = new HashMap();
    
    
    /**
     * Generates an xml file with a region and required role.
     * 
     * 
     * */
    public static void generateXml() throws TimeoutException, CacheWriterException, CacheExistsException, RegionExistsException, IOException{
		
		Cache cache = CacheUtil.createCache();
	    MembershipAttributes  ra = new MembershipAttributes (requiredRoles,
	            LossAction.RECONNECT, ResumptionAction.NONE);
	    AttributesFactory fac = new AttributesFactory();
	    fac.setMembershipAttributes(ra);
	    fac.setScope(Scope.DISTRIBUTED_ACK);
	    fac.setDataPolicy(DataPolicy.REPLICATE);

	    RegionAttributes attr = fac.createRegionAttributes();
	    File file = new File(xmlFileName);
	   // Cache cache = CacheFactory.create(dss);
	    Region region = CacheUtil.createRegion(regionName, attr);
	    //File file = new File("RoleReconnect-cache.xml");
	    try {
	      PrintWriter pw = new PrintWriter(new FileWriter(file), true);
	      CacheXmlGenerator.generate(cache, pw);
	      pw.close();
	      
	    }
	    catch (IOException ex) {
	      Log.getLogWriter().info("IOException during cache.xml generation to " + file +" : "+ ex);
	      throw ex;
	    }
	        
	    
	}
    /**
     * Generates an xml file with mutiple regions with required roles.
     * Every region defined has a required role played by some other vm 
     * in the system. When the xml is genrate all the role playin vms required
     * should be up and running otherwise will get a CacheClosedException,
     * 
     * @throws Exception.
     * */
    public static void generateXmlWithMultiRequiredRoles() throws Exception{
    	Cache cache = CacheUtil.createCache();
    	// regionA
    	MembershipAttributes  ra = new MembershipAttributes (roleRequiredRegionA,
	            LossAction.RECONNECT, ResumptionAction.NONE);
	    AttributesFactory fac = new AttributesFactory();
	    fac.setMembershipAttributes(ra);
	    fac.setScope(Scope.DISTRIBUTED_ACK);
	    fac.setDataPolicy(DataPolicy.REPLICATE);
	    RegionAttributes attr = fac.createRegionAttributes();
	    Region regionA = CacheUtil.createRegion(regionNameA, attr);
	    
	    
	    // regionB
	    ra = new MembershipAttributes (roleRequiredRegionB,
	            LossAction.RECONNECT, ResumptionAction.NONE);
	    fac = new AttributesFactory();
	    fac.setMembershipAttributes(ra);
	    fac.setScope(Scope.DISTRIBUTED_ACK);
	    fac.setDataPolicy(DataPolicy.REPLICATE);
	    attr = fac.createRegionAttributes();
	    Region regionB = CacheUtil.createRegion(regionNameB, attr);
	    
	    // regionC
	    ra = new MembershipAttributes (roleRequiredRegionC,
	            LossAction.RECONNECT, ResumptionAction.NONE);
	    fac = new AttributesFactory();
	    fac.setMembershipAttributes(ra);
	    fac.setScope(Scope.DISTRIBUTED_ACK);
	    fac.setDataPolicy(DataPolicy.REPLICATE);
	    attr = fac.createRegionAttributes();
	    Region regionC = CacheUtil.createRegion(regionNameC, attr);
	    
	    // regionD
	    
	    ra = new MembershipAttributes (roleRequiredRegionD,
	            LossAction.RECONNECT, ResumptionAction.NONE);
	    fac = new AttributesFactory();
	    fac.setMembershipAttributes(ra);
	    fac.setScope(Scope.DISTRIBUTED_ACK);
	    fac.setDataPolicy(DataPolicy.REPLICATE);
	    attr = fac.createRegionAttributes();
	    Region regionD = CacheUtil.createRegion(regionNameD, attr);
	    
	    // regionE
	    ra = new MembershipAttributes (roleRequiredRegionE,
	            LossAction.RECONNECT, ResumptionAction.NONE);
	    fac = new AttributesFactory();
	    fac.setMembershipAttributes(ra);
	    fac.setScope(Scope.DISTRIBUTED_ACK);
	    fac.setDataPolicy(DataPolicy.REPLICATE);
	    attr = fac.createRegionAttributes();
	    Region regionE= CacheUtil.createRegion(regionNameE, attr);
	    
	    // regionF
	    
	    ra = new MembershipAttributes (roleRequiredRegionF,
	            LossAction.RECONNECT, ResumptionAction.NONE);
	    fac = new AttributesFactory();
	    fac.setMembershipAttributes(ra);
	    fac.setScope(Scope.DISTRIBUTED_ACK);
	    fac.setDataPolicy(DataPolicy.REPLICATE);
	    attr = fac.createRegionAttributes();
	    Region regionF= CacheUtil.createRegion(regionNameF, attr);
	    
	    // regionG
	    
	    ra = new MembershipAttributes (roleRequiredRegionG,
	            LossAction.RECONNECT, ResumptionAction.NONE);
	    fac = new AttributesFactory();
	    fac.setMembershipAttributes(ra);
	    fac.setScope(Scope.DISTRIBUTED_ACK);
	    fac.setDataPolicy(DataPolicy.REPLICATE);
	    attr = fac.createRegionAttributes();
	    Region regionG= CacheUtil.createRegion(regionNameG, attr);
	    
	    generateXml(cache);
	   	
    }
    
    public static void generateXml(Cache cache) throws Exception{
    	//File file = new File("SelfRolePlaying.xml");
    	File file = new File(xmlFileName);
    	try {
  	      PrintWriter pw = new PrintWriter(new FileWriter(file), true);
  	      CacheXmlGenerator.generate(cache, pw);
  	      pw.close();
  	      
  	    }
  	    catch (IOException ex) {
  	      Log.getLogWriter().info("IOException during cache.xml generation to " + file +" : "+ ex);
  	      throw ex;
  	    }
    }
    
    /**
     * Generates the xml file used when the vm reconnects and throws exception
     * when not able to generate xml file.
     * 
     * @throws Exception
     * */
    public static void generateABRegionRequiredXml()throws Exception {
		Cache cache = CacheUtil.createCache();
	    MembershipAttributes  ra = new MembershipAttributes (roleRequiredRegionA,
	            LossAction.RECONNECT, ResumptionAction.NONE);
	    AttributesFactory fac = new AttributesFactory();
	    fac.setMembershipAttributes(ra);
	    fac.setScope(Scope.DISTRIBUTED_ACK);
	    fac.setDataPolicy(DataPolicy.REPLICATE);
	    RegionAttributes attr = fac.createRegionAttributes();
	    Region regionA = CacheUtil.createRegion(regionNameA, attr);
	    
	    // regionB
	    
	    ra = new MembershipAttributes (roleRequiredRegionB,
	            LossAction.RECONNECT, ResumptionAction.NONE);
	    fac = new AttributesFactory();
	    fac.setMembershipAttributes(ra);
	    fac.setScope(Scope.DISTRIBUTED_ACK);
	    fac.setDataPolicy(DataPolicy.REPLICATE);
	    attr = fac.createRegionAttributes();
	    Region regionB = CacheUtil.createRegion(regionNameB, attr);
	    
	    File file = new File(xmlFileNameRequireAB);
	    try {
		      PrintWriter pw = new PrintWriter(new FileWriter(file), true);
		      CacheXmlGenerator.generate(cache, pw);
		      pw.close();
		      
		    }
		catch (IOException ex) {
		     Log.getLogWriter().info("IOException during cache.xml generation to " + file +" : "+ ex);
		     throw ex;
		}
		
	}
	
    /**
     * Creates a cache playing the role of "rolePlayer" defined by hydra.GemFirePrms-roles 
     * property defined in reconnectRoleLossBackUP.conf.
     * 
     * @thorws TimeoutException, CacheWriterException, CacheExistsException, RegionExistsException
     * */
	public static void createRolePlayerVm() throws TimeoutException, CacheWriterException, CacheExistsException, RegionExistsException{
		//Properties props = new Properties();
        //props.setProperty(DistributionConfig.LOG_LEVEL_NAME, "fine");
        //props.setProperty(DistributionConfig.ROLES_NAME, rr1);

        //DistributedSystem ds = DistributedSystem.connect(props);
        Cache cache = CacheUtil.createCache();
        AttributesFactory fac = new AttributesFactory();
        fac.setScope(Scope.DISTRIBUTED_ACK);
        fac.setDataPolicy(DataPolicy.REPLICATE);

        RegionAttributes attr = fac.createRegionAttributes();
        Region region = CacheUtil.createRegion(regionName, attr);
        try{
        	Thread.sleep(60000);
        }catch(InterruptedException ignor){}
        region.put(key,value);
    }
	
	
	public static void createSingleRegionRequiresRoleAB() throws Exception{
		Cache cache = CacheUtil.createCache();
		String requiredRoles[] = new String[]{"relePlayerA", "rolePlayerB"};
		MembershipAttributes  ra = new MembershipAttributes (requiredRoles,
	            LossAction.RECONNECT, ResumptionAction.NONE);
		AttributesFactory fac = new AttributesFactory();
		fac.setMembershipAttributes(ra);
		fac.setScope(Scope.DISTRIBUTED_ACK);
        fac.setDataPolicy(DataPolicy.REPLICATE);

        RegionAttributes attr = fac.createRegionAttributes();
        Region region = CacheUtil.createRegion(regionNameA, attr);
        generateXml(cache);
	}
	/**
	 * Create a cache which playes the role of "rolePlayerA" which is required
	 * by the region created in this cache.
	 * 
	 * @throws Exception
	 * */
	public static void createSelfRolePlayingVm()throws Exception{
		Cache cache = CacheUtil.createCache();
		String requiredRoles[] = new String[] {"rolePlayerA"};
		MembershipAttributes  ra = new MembershipAttributes (requiredRoles,
	            LossAction.RECONNECT, ResumptionAction.NONE);
		AttributesFactory fac = new AttributesFactory();
		fac.setMembershipAttributes(ra);
		fac.setScope(Scope.DISTRIBUTED_ACK);
        fac.setDataPolicy(DataPolicy.REPLICATE);

        RegionAttributes attr = fac.createRegionAttributes();
        Region region = CacheUtil.createRegion(regionNameA, attr);
        generateXml(cache);
        
	}
	
	/**
	 * Creats a vm playing a role "rolePlayerA". The roles parameter
	 * is defined in the conf file for the test by specifing hydra.GemFirePrms-roles
	 * property for all the vms.
	 * 
	 * */
	public static void createRolePlayerAVm(){
		Cache cache = CacheUtil.createCache();
		AttributesFactory fac = new AttributesFactory();
        fac.setScope(Scope.DISTRIBUTED_ACK);
        fac.setDataPolicy(DataPolicy.REPLICATE);

        RegionAttributes attr = fac.createRegionAttributes();
        Region region = CacheUtil.createRegion(regionNameA, attr);
        
	}
	
	/**
	 * Creats a vm playing a role "rolePlayerB". The roles parameter
	 * is defined in the conf file for the test by specifing hydra.GemFirePrms-roles
	 * property for all the vms.
	 * 
	 * */
	
	public static void createRolePlayerBVm(){
		Cache cache = CacheUtil.createCache();
        AttributesFactory fac = new AttributesFactory();
        fac.setScope(Scope.DISTRIBUTED_ACK);
        fac.setDataPolicy(DataPolicy.REPLICATE);

        RegionAttributes attr = fac.createRegionAttributes();
        Region region = CacheUtil.createRegion(regionNameB, attr);
	}
	
	/**
	 * Creats a vm playing a role "rolePlayerC". The roles parameter
	 * is defined in the conf file for the test by specifing hydra.GemFirePrms-roles
	 * property for all the vms.
	 * 
	 * */
	
	public static void createRolePlayerCVm(){
		Cache cache = CacheUtil.createCache();
        AttributesFactory fac = new AttributesFactory();
        fac.setScope(Scope.DISTRIBUTED_ACK);
        fac.setDataPolicy(DataPolicy.REPLICATE);

        RegionAttributes attr = fac.createRegionAttributes();
        Region region = CacheUtil.createRegion(regionNameC, attr);
	}
	
	/**
	 * Creats a vm playing a role "rolePlayerD". The roles parameter
	 * is defined in the conf file for the test by specifing hydra.GemFirePrms-roles
	 * property for all the vms.
	 * 
	 * */
	public static void createRolePlayerDVm(){
		Cache cache = CacheUtil.createCache();
        AttributesFactory fac = new AttributesFactory();
        fac.setScope(Scope.DISTRIBUTED_ACK);
        fac.setDataPolicy(DataPolicy.REPLICATE);

        RegionAttributes attr = fac.createRegionAttributes();
        Region region = CacheUtil.createRegion(regionNameD, attr);
	}
	
	/**
	 * Creats a vm playing a role "rolePlayerE". The roles parameter
	 * is defined in the conf file for the test by specifing hydra.GemFirePrms-roles
	 * property for all the vms.
	 * 
	 * */
	public static void createRolePlayerEVm(){
		Cache cache = CacheUtil.createCache();
        AttributesFactory fac = new AttributesFactory();
        fac.setScope(Scope.DISTRIBUTED_ACK);
        fac.setDataPolicy(DataPolicy.REPLICATE);

        RegionAttributes attr = fac.createRegionAttributes();
        Region region = CacheUtil.createRegion(regionNameE, attr);
	}
	/**
	 * Creats a vm playing a role "rolePlayerF". The roles parameter
	 * is defined in the conf file for the test by specifing hydra.GemFirePrms-roles
	 * property for all the vms.
	 * 
	 * */
	public static void createRolePlayerFVm(){
		Cache cache = CacheUtil.createCache();
        AttributesFactory fac = new AttributesFactory();
        fac.setScope(Scope.DISTRIBUTED_ACK);
        fac.setDataPolicy(DataPolicy.REPLICATE);

        RegionAttributes attr = fac.createRegionAttributes();
        Region region = CacheUtil.createRegion(regionNameF, attr);
	}
	
	/**
	 * Creats a vm playing a role "rolePlayerG". The roles parameter
	 * is defined in the conf file for the test by specifing hydra.GemFirePrms-roles
	 * property for all the vms.
	 * 
	 * */
	public static void createRolePlayerGVm(){
		Cache cache = CacheUtil.createCache();
        AttributesFactory fac = new AttributesFactory();
        fac.setScope(Scope.DISTRIBUTED_ACK);
        fac.setDataPolicy(DataPolicy.REPLICATE);

        RegionAttributes attr = fac.createRegionAttributes();
        Region region = CacheUtil.createRegion(regionNameG, attr);
	}
	
	/**
	 * Creates a cache from an xml file specified by xmlFileName 
	 * expected to lose a required role.
	 * */
	public static void createCacheAndRegionsWithRoleLoss() throws Exception{
		Cache cache = CacheUtil.createCache(xmlFileName);
		try{
			Thread.sleep(60000);
		}catch(InterruptedException ignor){}
		try {
		  Region myRegion = cache.getRegion(regionName);
		  throw new Exception("The cache should have thrown CancelException");
			
		}
		catch (CancelException ignor){
			Log.getLogWriter().info("Got the expected CancelException exception ",ignor);
			
		}
		
	}
	
	/**
	 * Creates the cache from the xml file specified by xmlFileName.
	 * 
	 * @throws Exception.
	 * */
	public static void createCacheAndRegionsWithBackUp() throws Exception{
		Cache cache = CacheUtil.createCache(xmlFileName);
		//Region myRegion = cache.getRegion(regionName);
		try{
			Thread.sleep(60000);
		}catch(InterruptedException ignor){}
		Region myRegion = cache.getRegion(regionName);
		Set entrySet = myRegion.entrySet();
		Region.Entry keyValue = null;
		Iterator it = entrySet.iterator();
		while(it.hasNext()){
				keyValue = (Region.Entry)it.next();
				if ((((String)keyValue.getKey()).compareTo(key)) != 0){
				throw new Exception("The key doesnt match the orignal key : "+keyValue.getKey()+" Orignal Key : "+key);
				}
			
				if(((((String)keyValue.getValue()).compareTo(value)) != 0)){
					throw new Exception("The value doesnt match the orignal value for the key : "+keyValue.getValue()+" Orignal Key : "+value);
				}
			}
				
	}
	
	/**
	 * Creates a cache from a cache xml file specified by xmlFileNameRequireAB
	 * variable. Throws exception when not able to create a cache and tries for 
	 * more than 5 times due to a role loss.
	 * 
	 * throws Exception
	 * */
	
	public static void createCacheRequiresABFromXML() throws Exception{
		Cache cache = CacheUtil.createCache(xmlFileNameRequireAB);
		int attempts = 0;
		try{
			Thread.sleep(60000);
		}catch(InterruptedException ignor){}
		//cache = CacheFactory.getAnyInstance();
		while( attempts < 5){
			try{
				cache = CacheFactory.getAnyInstance();
				attempts++;
				Region myRegion = cache.getRegion(regionNameA);
                                Log.getLogWriter().info("Region : "+regionNameA+" created properly : "+myRegion);
				myRegion = cache.getRegion(regionNameB);
                                Log.getLogWriter().info("Region : "+regionNameB+" created properly : "+myRegion);
				break;
				//throw new Exception("The cache should have been closed because of role loss of : "+roleA);
			}
			catch (CancelException ignor){
				attempts++;
				try{
					Thread.sleep(10000);
				}catch(InterruptedException ign){}
				if (attempts == 5)
					throw new Exception("Treid reconnect : "+attempts+" times but failed to reconnect properly because of a role player member loss.");
			}
		}
		Log.getLogWriter().info("TOOK : "+attempts+"  ATTEMPTS TO RECONNECT AND CACHE : "+cache);
		
	}

       /**
       * Validates the cache creation and regions after it reconnects properly
       * to the distributed system.
       */
       public static void validateCacheCreation() throws Exception {
         int attempts = 0;
         try{
           Thread.sleep(60000);
         }catch(InterruptedException ignor){}
                //cache = CacheFactory.getAnyInstance();
         while( attempts < 5) {
           try {
             Cache cache = CacheFactory.getAnyInstance();
             attempts++;
             Region myRegion = cache.getRegion(regionNameA);
             validateRegion(myRegion);
             Log.getLogWriter().info("Region : "+regionNameA+" validated properly : "+myRegion);
             myRegion = cache.getRegion(regionNameB);
             validateRegion(myRegion);
             Log.getLogWriter().info("Region : "+regionNameB+" validated properly : "+myRegion);
             break;
                                //throw new Exception("The cache should have been closed because of role loss of : "+roleA);
             }
           catch (CancelException ignor){
               attempts++;
	       try{
                 Thread.sleep(10000);
                }catch(InterruptedException ign){}
                if (attempts == 5)
                  throw new Exception("Tried reconnect : "+attempts+" times but failed to reconnect properly because of a role player member loss.");
             }
          }// while loop
       }
	

         /**
	 * Creats the cache from the xml file specified by xmlFileName parameter.
	 * */
	public static void createMultiRegionRequireRoles(){
		Cache cache = CacheUtil.createCache(xmlFileName);
		Region region = cache.getRegion(regionNameA);
		
	}
	
	/**
	 * Validates the integrity of the region defined in the cache playing 
	 * the role required by the region.
	 * 
	 * @thorws Exception.
	 * */
	public static void validateSelfRolePlayerClient()throws Exception {
		Cache cache = CacheUtil.getCache();
		Region myRegion = cache.getRegion(regionNameA);
		validateRegion(myRegion);
	}
	
	/**
	 * Checks the integrity of all the regions by making sure there are 
	 * seven regions defined and all the regions have 20000 key-value pair. 
	 * If not then is throws an exception with appropriate message.
	 * 
	 * @throw Exception 
	 * */
	
	public static void checkGetInitialImageStats() throws Exception {
		//MasterController.sleepForMs(20000);
		Cache aCache = null;
		long time = System.currentTimeMillis();
		while (aCache == null){
			try{
				aCache = CacheFactory.getAnyInstance();
			}
			catch (CancelException ignor){
				if ((System.currentTimeMillis()-time) >240000 )
					throw new Exception("Waited 3 mins for the system to reconnect porperly");
				try{
					Thread.sleep(10000);
				}catch(InterruptedException ignor2){
					
				}
			}
		}
		try{
			Thread.sleep(60000);
		}catch(InterruptedException wait){}
                if ( ((GemFireCacheImpl)aCache).isClosed() ) {
                  Log.getLogWriter().severe("obtained closing cache from CacheFactory");
                }
		CachePerfStats stats = ((GemFireCacheImpl) aCache).getCachePerfStats();
		Log.getLogWriter().info("THE INITIMAGE STAT : "+ stats.getGetInitialImageKeysReceived());
		Log.getLogWriter().info("THE Number of Regions :"+stats.getRegions());
		if (stats.getRegions() != 7)
			throw new Exception("There should be 7 regions defined after reconnect");
		Set regions = aCache.rootRegions();
		Iterator it = regions.iterator();
		while(it.hasNext()){
			Region r = (Region)it.next();
			Set s = r.entrySet();
			if(s.size() != 20000){
				throw new Exception("There should be 20000 keys but found : "+s.size()+" in region : "+r);
			}
		Log.getLogWriter().info("Region : "+r+" contains : "+s.size()+" keys and value pairs");
		}
		
		//		System.out.println("The INITIMAGE STAT XXXXX  : "+TestHelper.getStat_getInitialImageKeysReceived());
//		Cache cache = CacheFactory.getAnyInstance();
		
	}
	
	/**
	 * Validate all the key values are present in the region and it dint reconnected.
	 * 
	 * @throws Exception.
	 * */
	private static void validateRegion(Region myRegion) throws Exception{
		//String key = "myKey";
		//String value = "myValue";
		int i =0;
		//HashMap regionKeyValue = (HashMap)region2HashMap.get(myRegion);
		HashMap regionKeyValue = (HashMap)((SustainabilityBB.getBB()).getSharedMap()).get(myRegion.getName());
		Set entrySet = myRegion.entrySet();
		Iterator it = entrySet.iterator();
		Log.getLogWriter().info("Checking integrity for : "+myRegion.getName());
		if (entrySet.size() != regionKeyValue.size()){
			throw new Exception("There should be : "+regionKeyValue.size()+" key-value pairs but found : "+entrySet.size());
		}
		Log.getLogWriter().info("ENTRY KEY SIZE : "+entrySet.size()+" and the hashmap : "+regionKeyValue.size());
		Region.Entry keyValue= null;
		while (it.hasNext()){
			keyValue = (Region.Entry)it.next();
			String value= (String)regionKeyValue.get(keyValue.getKey());
			if(value == null){
				throw new RuntimeException("No key-value pair for the : "+keyValue.getKey()+" in KeyValue HashMap but the key is present in the local region");
			}else if(value.compareTo((String)keyValue.getValue())!= 0){
				throw new RuntimeException("The value for the key : "+keyValue.getKey()+" from entry set :"+keyValue.getKey()+" does not match the value from the hashmap : "+value);
			}
						
		}
	}
	
	/**
	 * Puts some key value pair in regionA.
	 * 
	 * @throws Exception.
	 * */
	public static void putRegionA() throws Exception{
		
		String key = "myKey";
		String value = "myValue";
		HashMap regionKeyValue = new HashMap();
		Cache cache= null;
		while(cache == null){
			try{
				cache = CacheFactory.getAnyInstance();
			}
			catch (CancelException ignor){
				try{
					Thread.sleep(100);
				}catch(InterruptedException ign){}
			}
		}
		Region myRegion = cache.getRegion(regionNameA);
		for(int i =0; i < 10 ; i ++){
			myRegion.put((key+i),(value+i));
			regionKeyValue.put((key+i),(value+i));
		}
				
				
		SharedMap smap = (SustainabilityBB.getBB()).getSharedMap();
		smap.put(myRegion.getName(), regionKeyValue );
		try {
			Thread.sleep(10000);
		}catch(InterruptedException ignor){}
		
	}
	
	/**
	 * Puts some keys value in regionB.
	 * 
	 * @throws Exception.
	 * */
	public static void putRegionB() throws Exception{
		
		String key = "myKey";
		String value = "myValue";
		HashMap regionKeyValue = new HashMap();
		Cache cache= null;
		while(cache == null){
			try{
				cache = CacheFactory.getAnyInstance();
			}
			catch (CancelException ignor){
				try{
					Thread.sleep(100);
				}catch(InterruptedException ign){}
			}
		}
		Region myRegion = null;
		
		while(myRegion == null){
			myRegion= cache.getRegion(regionNameB);
		}
		for(int i =0; i < 10 ; i ++){
			myRegion.put((key+i),(value+i));
			regionKeyValue.put((key+i),(value+i));
		}
		
		//region2HashMap.put(myRegion, regionKeyValue);
		
		SharedMap smap = (SustainabilityBB.getBB()).getSharedMap();
		smap.put(myRegion.getName(), regionKeyValue );
		try {
			Thread.sleep(10000);
		}catch(InterruptedException ignor){}
	}
	
	/**
	 * Puts multiple keys value pair in RegionA.
	 * */
	public static void putKeysValuesRegionA() throws Exception{
		Cache cache = CacheUtil.createCache();
		AttributesFactory fac = new AttributesFactory();
        fac.setScope(Scope.DISTRIBUTED_ACK);
        fac.setDataPolicy(DataPolicy.REPLICATE);

        RegionAttributes attr = fac.createRegionAttributes();
        Region region = CacheUtil.createRegion(regionNameA, attr);
        putKeysValues(region);
        
    }
	
	/**
	 * Puts multiple keys value pair in RegionB.
	 * */
	
	public static void putKeysValuesRegionB() throws Exception{
		Cache cache = CacheUtil.createCache();
		AttributesFactory fac = new AttributesFactory();
        fac.setScope(Scope.DISTRIBUTED_ACK);
        fac.setDataPolicy(DataPolicy.REPLICATE);

        RegionAttributes attr = fac.createRegionAttributes();
        Region region = CacheUtil.createRegion(regionNameB, attr);
        putKeysValues(region);
        
    }
	
	/**
	 * Puts multiple keys value pair in RegionC.
	 * */
	public static void putKeysValuesRegionC() throws Exception{
		Cache cache = CacheUtil.createCache();
		AttributesFactory fac = new AttributesFactory();
        fac.setScope(Scope.DISTRIBUTED_ACK);
        fac.setDataPolicy(DataPolicy.REPLICATE);

        RegionAttributes attr = fac.createRegionAttributes();
        Region region = CacheUtil.createRegion(regionNameC, attr);
        putKeysValues(region);
        
    }
	
	/**
	 * Puts multiple keys value pair in RegionD.
	 * */
	public static void putKeysValuesRegionD() throws Exception{
		Cache cache = CacheUtil.createCache();
		AttributesFactory fac = new AttributesFactory();
        fac.setScope(Scope.DISTRIBUTED_ACK);
        fac.setDataPolicy(DataPolicy.REPLICATE);

        RegionAttributes attr = fac.createRegionAttributes();
        Region region = CacheUtil.createRegion(regionNameD, attr);
        putKeysValues(region);
        
    }
	
	/**
	 * Puts multiple keys value pair in RegionE.
	 * */
	public static void putKeysValuesRegionE() throws Exception{
		Cache cache = CacheUtil.createCache();
		AttributesFactory fac = new AttributesFactory();
        fac.setScope(Scope.DISTRIBUTED_ACK);
        fac.setDataPolicy(DataPolicy.REPLICATE);

        RegionAttributes attr = fac.createRegionAttributes();
        Region region = CacheUtil.createRegion(regionNameE, attr);
        putKeysValues(region);
        
    }
	
	/**
	 * Puts multiple keys value pair in RegionF.
	 * */
	public static void putKeysValuesRegionF() throws Exception{
		Cache cache = CacheUtil.createCache();
		AttributesFactory fac = new AttributesFactory();
        fac.setScope(Scope.DISTRIBUTED_ACK);
        fac.setDataPolicy(DataPolicy.REPLICATE);

        RegionAttributes attr = fac.createRegionAttributes();
        Region region = CacheUtil.createRegion(regionNameF, attr);
        putKeysValues(region);
        
    }
	
	/**
	 * Puts multiple keys value pair in RegionG.
	 * */
	public static void putKeysValuesRegionG() throws Exception{
		Cache cache = CacheUtil.createCache();
		AttributesFactory fac = new AttributesFactory();
        fac.setScope(Scope.DISTRIBUTED_ACK);
        fac.setDataPolicy(DataPolicy.REPLICATE);

        RegionAttributes attr = fac.createRegionAttributes();
        Region region = CacheUtil.createRegion(regionNameG, attr);
        putKeysValues(region);
        
    }
	
	/**
	 * Puts 20000 keys value for the region passed as parameter to 
	 * the method.
	 * 
	 * @param region {@link com.gemstone.gemfire.cache.Region}
	 * */
	public static void putKeysValues(Region theRegion) throws Exception{
		String key = "Key";
        String value = "Value";
        for (int i =0; i < 20000; i++){
        	theRegion.put((key+i), (value+i)); 
        }
	}
	/***
	 * Stops and restarts a client vm.
	 * @throws Exception
	 * /	
	public static void stopAndRestartClient()throws Exception{
		
		stopClient();
		MasterController.sleepForMs(5000);
		startClient();
		MasterController.sleepForMs(10000);
	}
	
	/**
	 * Stops all the clients listed in the roles.RolesPrms-clientsToStop property
	 * from the conf file.
	 * 
	 * @throws ClientVmNotFoundException.
	 * */
	public static void stopOtherClients() throws ClientVmNotFoundException{
		Vector clientsToStop = TestConfig.tab().vecAt(RolesPrms.clientsToStop);
		for(int i = 0; i < clientsToStop.size() ; i++){
			String stopMode = TestConfig.tab().stringAt(RolesPrms.stopMode, "MEAN_KILL" );
		    String startMode = TestConfig.tab().stringAt(RolesPrms.startMode, "ON_DEMAND" );
		    Long key = RolesPrms.clientsToStop;
		    String clientToStop = (String)clientsToStop.elementAt(i);
		    Log.getLogWriter().info("STOPPING THE CLIENT : "+clientToStop);
		    ClientVmInfo info = ClientVmMgr.stop(
		                             "stop client", 
		                             ClientVmMgr.toStopMode( stopMode ),
		                             ClientVmMgr.toStartMode( startMode),
		                             new ClientVmInfo(null, clientToStop, null)
		                        );
		    Log.getLogWriter().info("stopped client" + info);
		}
			
	}
	
	/**
	 * Stops and restarts a random client from a vector of clients 
	 * configured in the conf file by "roles.RolesPrms-clientsToStop"
	 * property. This task is called multiple times from "reconnectRandomRolePlayerShutDown.conf".
	 * */
	
	public static void stopAndRestartRandomRolePlayingClients() throws ClientVmNotFoundException, Exception{
		Vector clientsToStop = TestConfig.tab().vecAt(RolesPrms.clientsToStop);
		int numClients = clientsToStop.size();
		Random rand = new Random();
		int randClientNum = rand.nextInt(numClients);
		String clientToStopAndRestart = (String)clientsToStop.elementAt(randClientNum);
		//((SustainabilityBB.getBB()).getSharedMap()).put("clientStopAndStarted",clientToStopAndRestart);
		Log.getLogWriter().info("STOPPING AND STARTING CLIENT : "+clientToStopAndRestart);
		stopClient(clientToStopAndRestart);
		long time = System.currentTimeMillis();
		MasterController.sleepForMs(1000);
		startClient(clientToStopAndRestart);
		Log.getLogWriter().info("TIME TAKE TO RESTART THE CLIENT : "+(System.currentTimeMillis()-time));
		MasterController.sleepForMs(60000);
//		checkGetInitialImageStats();
	}
	
	/**
	 * Stops a particular client specified by the parameter.
	 * 
	 * @throws ClientVmNotFoundException
	 * */
	public static void stopClient(String theClient) throws ClientVmNotFoundException{
		String stopMode = TestConfig.tab().stringAt(RolesPrms.stopMode, "MEAN_KILL" );
	    String startMode = TestConfig.tab().stringAt(RolesPrms.startMode, "ON_DEMAND" );
	    Long key = RolesPrms.clientsToStop;
	    ClientVmInfo info = ClientVmMgr.stop(
                "stop client", 
                ClientVmMgr.toStopMode( stopMode ),
                ClientVmMgr.toStartMode( startMode),
                new ClientVmInfo(null, theClient, null)
           );
	    Log.getLogWriter().info("STOPED : "+info);
	    
	}
	public static void startClient(String theClient){
		try {
			
		   ClientVmInfo info = ClientVmMgr.start("start client", new ClientVmInfo(null, theClient, null));
		   Log.getLogWriter().info("RESTARTED CLIENT  : "+info);
		   
			
		} catch( ClientVmNotFoundException e ) {
		   Log.getLogWriter().info("Could not start client!");
		}
		
	}
	
	
	/**
	 * Prints all the root regions in the cache. Helps while 
	 * debugging reconnect issues with integrity problems.
	 * */
	public static void printRegions() throws Exception {
		Cache cache = (GemFireCacheImpl)CacheFactory.getAnyInstance();
		   
		   Set roots = cache.rootRegions();
		   Iterator it = roots.iterator();
		   while (it.hasNext()){
			   Log.getLogWriter().info("ROOT XXXX : "+it.next());
		   }
	}
	
	/**
	 * Stops a client specified by the roles.RolesPrms-clientsToStop parameter.
	 * 
	 * @thorws ClientVmNotFoundException.
	 * */
	public static void stopClient() throws ClientVmNotFoundException {
		    
		 	String stopMode = TestConfig.tab().stringAt(RolesPrms.stopMode, "MEAN_KILL" );
		    String startMode = TestConfig.tab().stringAt(RolesPrms.startMode, "ON_DEMAND" );
		    Long key = RolesPrms.clientsToStop;
		    String clientToStop = TestConfig.tasktab().stringAt(key, TestConfig.tab().stringAt(key));
		    ClientVmInfo info = ClientVmMgr.stop(
		                             "stop client", 
		                             ClientVmMgr.toStopMode( stopMode ),
		                             ClientVmMgr.toStartMode( startMode),
		                             new ClientVmInfo(null, clientToStop, null)
		                        );
		    
		    Log.getLogWriter().info("stopped client" + info);
		   // long count = RolesBB.getBB().getSharedCounters().incrementAndRead(RolesBB.expectedRoleLossEvents);
		    //Log.getLogWriter().info("after incrementing counter, RolesBB.expectedRoleLossEvents = " + count);
		  }
	 
	 public static void startClient() throws ClientVmNotFoundException {
		try {
			
		   ClientVmInfo info = ClientVmMgr.start("start client");
		   Log.getLogWriter().info("RESTARTED CLIENT : "+info);
		} catch( ClientVmNotFoundException e ) {
		   Log.getLogWriter().info("Could not start client!");
		}
		//long count = RolesBB.getBB().getSharedCounters().incrementAndRead(RolesBB.expectedRoleGainEvents);
		//Log.getLogWriter().info("after incrementing counter, RolesBB.expectedRoleGainEvents = " + count);
	}
	 
	/**
	 * Keeps the vm alive for hydra.
	 * */
	 public static void doSomeThing(){
		 try{
			 Thread.sleep(60000);
		 }catch(InterruptedException ignor){}
	 }
	
	
	
}
