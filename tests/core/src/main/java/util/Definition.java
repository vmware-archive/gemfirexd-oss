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
import java.util.*;

/** Abstract class used to define a valid combination of attributes for
 *  something such as a region or cache.
 */
public abstract class Definition implements java.io.Serializable {

// name given to this definition in the region spec
protected String specName; 

// other variables used internally
protected Map specMap; 
protected static GsRandom RegDefRand = TestConfig.tab().getRandGen();

// used for getting tokens
protected static String SPEC_NAME_DELIM = "[\\s]*([s|S][p|P][e|E][c|C][n|N][a|A][m|M][e|E])[\\s]*=[\\s]*"; 
        // any amount of white space followed by "nameSpec" (in any case) followed by any amount of 
        // white space followed by = followed by any amount of white space
protected static String WHITE_SPACE_DELIM = "[\\s]+"; // separates white space
protected static String ATTR_DELIM = "[\\s]+|[,][\\s]+";  // separates attributes within a single spec
protected static String SPEC_TERMINATOR = ":";        // ends an attribute within a spec

abstract protected void initializeWithSpec(String specStr);
abstract public String toString();

//================================================================================
// protected methods

/** 
 * Fill in this instance of Definition based on the spec named
 * by <code>specName</code>.
 *
 * @throws TestException if there is no spec named <code>specName</code>
 */
protected void getDefinition(Long hydraSpecParam, String specName) {
   initializeSpecMap(hydraSpecParam);
   String specStr = (String)specMap.get(specName);
   if (specStr == null) {
      StringBuffer errStr = new StringBuffer();
      errStr.append("Did not find specName " + specName + "; defined specNames are ");
      Iterator it = specMap.keySet().iterator();
      while (it.hasNext()) {
         errStr.append(" " + it.next());
      }
      throw new TestException(errStr.toString());
   }
   initializeWithSpec(specStr);
}


/** 
 *  Initialize the specMap with the spec String read from hydra parameters.
 *  Keys are specNames, values are a String containing the region spec for 
 *  that specName.
 *
 *  @param hydraSpecParam - the hydra parameter containing attributes specifications.
 */
protected synchronized void initializeSpecMap(Long hydraSpecParam) {
   if (specMap != null) // already initialized
      return;
   String spec = TestConfig.tab().stringAt(hydraSpecParam);
   specMap = new HashMap();
   Log.getLogWriter().fine("Initializing spec map...");
   String[] specArr = spec.split(SPEC_NAME_DELIM);
   Log.getLogWriter().fine("After splitting to get separate specs, specArr is length " + specArr.length);
   for (int i = 0; i < specArr.length; i++) { // iterate through each specification
      String specStr = specArr[i]; 
      Log.getLogWriter().fine("sp[" + i + "] is *" + specStr + "*");
      if (specStr.length() == 0)
         continue;
      String[] tokensArr = specStr.split(ATTR_DELIM);
      Log.getLogWriter().fine("   After splitting to get first token, tokensArr.length is " + tokensArr.length);
      for (int j = 0; j < tokensArr.length; j++) {
          Log.getLogWriter().fine("   tokens[" + j + "] is #" + tokensArr[j] + "#]");
      }
      String specName = tokensArr[0];
      if (specName.length() == 0)
         throw new TestException("SpecName is empty *" + specName + "*");
      if (specName.endsWith(SPEC_TERMINATOR))
         specName = specName.substring(0, specName.length() - 1);
      specMap.put(specName, specStr);   
   }
   Log.getLogWriter().fine("SpecMap size is " + specMap.size());
   Iterator it = specMap.keySet().iterator();
   while (it.hasNext()) {
      Object key = it.next();
      Log.getLogWriter().fine("   Key: *" + key + "*, value *" + specMap.get(key) + "*");
   }
}

// An array of tokens and a tokenIndex into that array, used by tokenizer methods below.
protected String[] tokenArr;
protected int tokenIndex = -1;

/** Return the next token */
protected String getNextToken() {
   if ((tokenIndex + 1) < tokenArr.length) {
      tokenIndex++;
      String token = tokenArr[tokenIndex];
      if (token.equals(""))
         return getNextToken();
      Log.getLogWriter().fine("Get next token, token is " + token);
      return token;
   }
   return null;
}

///** Return the current token */
//protected String getCurrentToken() {
//   if (tokenIndex < tokenArr.length) {
//      String token = tokenArr[tokenIndex];
//      Log.getLogWriter().fine("Get current token, token is " + token);
//      return token;
//   }
//   throw new TestException("Requesting current token, but already at end " + tokensToString());
//}

/** Get the next tokean and make sure it is an = */
protected String getTokenEquals() {
   String token = getNextToken();
   if (!token.equals("="))
      throw new TestException("Expected token =, but got " + token + " from " + tokensToString());
   Log.getLogWriter().fine("Got equals token " + token);
   return token;
}

/** Get the next 1 or more tokens and return as a List. The list
 *  of tokens ends when either there are no more tokens, or a token is
 *  followed by :
 */
protected List getTokenList() {
   Log.getLogWriter().fine("Getting token list...");
   ArrayList aList = new ArrayList();
   do {
      String token = getNextToken();
      if (token == null) break;
      if (token.equals(SPEC_TERMINATOR)) break;
      if (token.endsWith(SPEC_TERMINATOR)) {
         aList.add(token.substring(0, token.length() - 1));
         break;
      } else
         aList.add(token);
   } while (true);
   if (aList.size() == 0)
      throw new TestException("Missing attributes " + tokensToString());
   for (int i = 0; i < aList.size(); i++) {
      Log.getLogWriter().fine("List(" + i + ") is " + aList.get(i));
   }
   return aList;
}  

/** Get the next 1 or more tokens and randomly choose one. The list
 *  of tokens ends when either there are no more tokens, or a token is
 *  followed by :
 */
protected String getTokenFromList() {
   List aList = getTokenList();
   int randInt = RegDefRand.nextInt(0, aList.size() - 1);
   return (String)(aList.get(randInt));
}  

/** Return a Boolean (or null) from the next token or list of tokens */
protected Boolean getTokenBoolean() {
   String token = getTokenFromList();
   if (token.equalsIgnoreCase("null"))
      return null;
   Log.getLogWriter().fine("Chose boolean " + token);
   return new Boolean(token);
}

/** Return a String (or null) from the next token or list of tokens */
protected String getTokenString() {
   String token = getTokenFromList();
   if (token.equalsIgnoreCase("null"))
      return null;
   Log.getLogWriter().fine("Chose string " + token);
   return token;
}

/** Return an Integer (or null) from the next token or list of tokens */
protected Integer getTokenInteger() {
   String token = getTokenFromList();
   if (token.equalsIgnoreCase("null"))
      return null;
   Log.getLogWriter().fine("Chose integer " + token);
   return new Integer(token);
}

/** Return a Long (or null) from the next token or list of tokens */
protected Long getTokenLong() {
   String token = getTokenFromList();
   if (token.equalsIgnoreCase("null"))
      return null;
   Log.getLogWriter().fine("Chose long " + token);
   return new Long(token);
}

/** Return a Float (or null) from the next token or list of tokens */
protected Float getTokenFloat() {
   String token = getTokenFromList();
   if (token.equalsIgnoreCase("null"))
      return null;
   Log.getLogWriter().fine("Chose float " + token);
   return new Float(token);
}

/** Return a Class (or null) from the next token or list of tokens */
protected Class getTokenClass() {
   String token = getTokenFromList();
   if (token.equalsIgnoreCase("null"))
      return null;
   try {
      Log.getLogWriter().fine("Chose class " + token);
      return Class.forName(token);
   } catch (ClassNotFoundException e) {
      throw new TestException("Could not find specified class: " + token + "\n" + tokensToString() + TestHelper.getStackTrace(e));
   }
}

protected String tokensToString() {
   StringBuffer aStr = new StringBuffer();
   for (int i = 0; i < tokenArr.length; i++) {
      if (i == tokenIndex)
         aStr.append("<<ERROR near here>>>");
      aStr.append(tokenArr[i] + " ");
   }
   return aStr.toString();
}

}
