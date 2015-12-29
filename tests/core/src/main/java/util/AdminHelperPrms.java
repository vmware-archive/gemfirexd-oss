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

package util;

import hydra.*;
import com.gemstone.gemfire.admin.*;

/**
 *  A class used to store keys related to the util.AdminHelper class.
 * 
 *
 */

public class AdminHelperPrms extends BasePrms {

   /**
    * (boolean) 
    *
    * Create an AdminDistributedSystem in the same VM as a GemFire Distributed 
    * System.  Defaults to false.
    */
   public static Long adminInDsVm;
   public static boolean adminInDsVm() {
     Long key = adminInDsVm;
     return tasktab().booleanAt(key, tab().booleanAt(key, false));
   }

   /**
    * (int)
    * adminInterface to use (admin or jmx)
    * Defaults to admin
    */
   public static Long adminInterface;
 
   public static final int ADMIN = 0;
   public static final int JMX   = 1;

   public static int getAdminInterface() {
     Long key = adminInterface;
     String val = tab().stringAt( key, "ADMIN" );
     if ( val.equalsIgnoreCase( "ADMIN" )) {
       return ADMIN;
     } else if (val.equalsIgnoreCase( "JMX" )) {
       return JMX;
     } else {
       throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": " + val);
     }
   }

   /**
    *  (SystemMembershipListener)
    *  Fully qualified pathname of a SystemMembershipListener.  See interface
    *  in {@link com.gemstone.gemfire.admin.SystemMembershipListener}.
    *
    *  Default value is null (we won't install an SystemMembershipListener unless
    *  requested).
    */
   public static Long systemMembershipListener;
   public static SystemMembershipListener getSystemMembershipListener() {
   Long key = systemMembershipListener;
     String val = tasktab().stringAt( key, tab().stringAt( key, null ) );
     try {
       return (SystemMembershipListener)instantiate( key, val );
     } catch( ClassCastException e ) {
       throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": " + val + " does not implement SystemMembershipListener", e );
     }
   }

   /**
    *  (SystemMemberCacheListener)
    *  Fully qualified pathname of a SystemMemberCacheListener.  See interface
    *  in {@link com.gemstone.gemfire.admin.SystemMemberCacheListener}.
    *
    *  Default value is null (we won't install an SystemMemberCacheListener unless
    *  requested).
    */
   public static Long systemMemberCacheListener;
   public static SystemMemberCacheListener getSystemMemberCacheListener() {
   Long key = systemMemberCacheListener;
     String val = tasktab().stringAt( key, tab().stringAt( key, null ) );
     try {
       return (SystemMemberCacheListener)instantiate( key, val );
     } catch( ClassCastException e ) {
       throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": " + val + " does not implement SystemMemberCacheListener", e );
     }
   }

   /**
    *  (AlertListener)
    *  Fully qualified pathname of an AlertListener.  See interface
    *  in {@link com.gemstone.gemfire.admin.AlertListener}.
    *
    *  Default value is null (we won't install an AlertListener unless   
    *  requested).
    */
   public static Long alertListener;
   public static AlertListener getAlertListener() {
   Long key = alertListener;
     String val = tasktab().stringAt( key, tab().stringAt( key, null ) );
     try {
       return (AlertListener)instantiate( key, val );
     } catch( ClassCastException e ) {
       throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": " + val + " does not implement AlertListener", e );
      }
    }

   //------------------------------------------------------------------------
   // Utility methods
   //------------------------------------------------------------------------

   private static Object instantiate( Long key, String classname ) {
     if ( classname == null ) {
       return null;
     }
     try {
       Class cls = Class.forName( classname );
       return cls.newInstance();
     } catch( Exception e ) {
       throw new HydraConfigException( "Illegal value for " + nameForKey( key
 ) + ": cannot instantiate " + classname, e );
     }
   }

   static {
       setValues( AdminHelperPrms.class );
   }
   public static void main( String args[] ) {
       dumpKeys();
   }
}
