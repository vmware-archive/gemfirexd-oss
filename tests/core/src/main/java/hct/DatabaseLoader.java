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

package hct;

import com.gemstone.gemfire.cache.*;
import java.util.*;
import java.io.*;
import hydra.*;
import objects.*;

/**
 * A <code>CacheLoader</code> that simulates a database.  The {@link
 * #load} method recognizes special keys that will "disable" (and
 * subsequently "enable") the database.  The database will not service
 * requests while it is diabled.  (Or rather, it will take longer than
 * the {@linkplain PoolDescription#getReadTimeout timeout
 * interval} to service the request.)
 *
 * @author Belinda Buenafe
 * @since 2.0.2
 */
public class DatabaseLoader implements CacheLoader, Declarable
{

  static GsRandom rand = new GsRandom();
  static boolean isDisabled;
  static TestConfig config = TestConfig.getInstance();
  static ConfigHashtable tab = TestConfig.tab();
	
  public void init( Properties p ) {
  }

  public void close() {
  }

  /**
   * Loads an object from the "database".  Simulates the overhead of a
   * database call by waiting a {@link HctPrms#dbLoadTimeMs given
   * number} of milliseconds.
   *
   * @see DatabaseLoader
   */
  public Object load(LoaderHelper helper) throws CacheLoaderException {
		
     String key = (String) helper.getKey();
     if (key.equals("disableDatabase"))
        isDisabled = true;
     if (key.equals("enableDatabase"))
        isDisabled = false;

     if (isDisabled) {
        int timeout = config.getPoolDescription("brloader")
                            .getReadTimeout().intValue();
        MasterController.sleepForMs(timeout + 200);
        return null;
     }

     Object newObj = null;
     String objtype = tab.stringAt(HctPrms.objectType);
     String index = key.substring(key.indexOf('_') + 1);
     if (objtype.equals("bytearray") || objtype.equals("string")) {
        newObj = SizedString.init( Integer.parseInt(index) );
        if (objtype.equals("bytearray"))
           newObj = ((String) newObj).getBytes();
     } else if (objtype.equals("xmlstring")) {
        newObj = makeDataString(key);
     } else {
        Class c;
        try {
           c = Class.forName(objtype);
        } catch (ClassNotFoundException nfe) {
           throw new CacheLoaderException("Database loader cannot find class "
               + objtype);
        } catch (Exception ex) {
           throw new CacheLoaderException("Error for class " + objtype, ex);
        }

        try {
           newObj = c.newInstance();
           ((ConfigurableObject) newObj).init( Integer.parseInt(index) );
        } catch (NumberFormatException e) {
           throw new CacheLoaderException("Could not get index for key: " + key);
        } catch (Exception ex) {
           throw new CacheLoaderException("Error creating instance of " + objtype, ex);
        } 
     }
     int sleepMs = tab.intAt(HctPrms.dbLoadTimeMs);
     if (sleepMs > 0)
        MasterController.sleepForMs(sleepMs);
     if (tab.booleanAt(HctPrms.debug))
        System.out.println("DatabaseLoader returning " + newObj + " for key " + key);
     return newObj;
  }


  /**
   * A little class used to generate XML data
   */
  static class NodeData implements Serializable {
    public boolean isDeposit;
    public String id = "none";
    public int dollars;
    public int cents;
    public int agent;
    public int expenseType;
    public String toString() {
      StringBuffer buf = new StringBuffer();
      if (isDeposit)
        buf.append("deposit, $");
      else
        buf.append("withdrawal, $");
      buf.append (dollars + "." + cents);
      buf.append (", agent " + agent);
      buf.append (", expenseType " + expenseType);
      return buf.toString();
    }
  }

  private static String makeDataString(String objName) {
     NodeData nd = chooseNodeValues();
     nd.id = objName;
     return createNodeFromData (nd);
  }

  /**
   * Creates a new, random <code>NodeData</code>
   */
  private static NodeData chooseNodeValues() {
     NodeData vals = new NodeData();
     vals.isDeposit = rand.nextBoolean();
     vals.dollars = rand.nextInt(3000);
     vals.cents = rand.nextInt(99);
     vals.expenseType = -1;
     if (vals.isDeposit)
       vals.agent = 14 + rand.nextInt(2);
     else {
       vals.agent = rand.nextInt(13);
       vals.expenseType = rand.nextInt(20);
     }
     return vals;
  }

  /**
   * Returns an XML encoding of a <code>NodeData</code>
   */
  public static String createNodeFromData(NodeData nd) {

     // Create a string representation of the node
     StringBuffer buf = new StringBuffer();
     buf.append("***" + nd.id + "***");
     buf.append( "<?xml version=\"1.0\"?>" );
     if (nd.isDeposit)
        buf.append("<deposit id=\"" + nd.id + "\">");
     else {
        buf.append("<withdrawal id=\"" + nd.id + "\">");
        buf.append("<expenseType>");
        buf.append(nd.expenseType);
        buf.append("</expenseType>");
     }
     buf.append("<amount isBig=\"" + (nd.dollars >= 2000) );
     buf.append("\"> ");
     buf.append(nd.dollars + "." + nd.cents);
     buf.append("</amount>");

     buf.append("<description>");
     int agent = nd.agent;
     if (agent == 0)
        buf.append("<name>Fred Meyer</name> <addr>aaaaaaaaa</addr>");
     else if (agent == 1)
        buf.append("<name>Fred Meyer</name> <addr>bbbbbbbbb</addr>");
     else if (agent == 2)
        buf.append("<name>Fred Meyer</name> <addr>ccccccccc</addr>");
     else if (agent == 3)
        buf.append("<name>Fred Meyer</name> <addr>ddddddddd</addr>");
     else if (agent == 4)
        buf.append("<name>Fred Meyer</name> <addr>eeeeeeeee</addr>");
     else if (agent == 5)
        buf.append("<name>Target</name> <addr>fffffffff</addr>");
     else if (agent == 6)
        buf.append("<name>Target</name> <addr>ggggggggg</addr>");
     else if (agent == 7)
        buf.append("<name>Target</name> <addr>hhhhhhhhh</addr>");
     else if (agent == 8)
        buf.append("<name>Target</name> <addr>iiiiiiiii</addr>");
     else if (agent == 9)
        buf.append("<name>Target</name> <addr>jjjjjjjjj</addr>");
     else if (agent == 10)
        buf.append("<name>SafewayA</name> <addr>kkkkkkkkk</addr>");
     else if (agent == 11)
        buf.append("<name>SafewayB</name> <addr>lllllllll</addr>");
     else if (agent == 12)
        buf.append("<name>Safeway</name> <addr>mmmmmmmmm</addr>");
     else if (agent == 13)
        buf.append("<name>Safeway</name> <addr>nnnnnnnnn</addr>");
     else if (agent == 14)
        buf.append("<name>MakeMoreMoney,Inc.</name> <addr>ooooooooo</addr>");
     else if (agent == 15)
        buf.append("<name>\"Social Security\"</name> <addr>nnnnnnnnn</addr>")
;
     else if (agent == 16)
        buf.append("<name>'\"Rich Uncle\"</name> <addr>nnnnnnnnn</addr>");
     buf.append("</description>");

     if (nd.isDeposit)
        buf.append("</deposit>");
     else
        buf.append("</withdrawal>");

     return buf.toString();

  }




   	
}
