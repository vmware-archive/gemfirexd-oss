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

package hydra;

import bsh.EvalError;
import bsh.Interpreter;

import com.gemstone.gemfire.LogWriter;

import java.io.*;
import java.text.*;
import java.util.*;

/** 
 * Parses hydra config files.
 */
public class ConfigParser {

// @todo lises guard against cycles in include files

  private static final String INCLUDE          = "include";
  private static final String RANDOMINCLUDE    = "randomInclude";
  private static final String INCLUDEIFPRESENT = "includeIfPresent";

  private static final String UNITTEST         = "unittest";
  private static final String TESTCLASS        = "testClass";
  private static final String TESTMETHOD       = "testMethod";

  private static final String STARTTASK        = "starttask";
  private static final String INITTASK         = "inittask";
  private static final String TASK             = "task";
  private static final String CLOSETASK        = "closetask";
  private static final String ENDTASK          = "endtask";

  private static final String TASKCLASS        = "taskClass";
  private static final String TASKMETHOD       = "taskMethod";

  private static final String SEQUENTIAL       = "sequential";
  private static final String BATCH            = "batch";

  private static final String CLIENTNAMES      = "clientNames";

  private static final String RUNMODE          = "runMode";
  private static final String ALWAYS           = "always";
  private static final String ONCE             = "once";
  private static final String DYNAMIC          = "dynamic";

  private static final String MAXTIMESTORUN    = "maxTimesToRun";
  private static final String MAXTHREADS       = "maxThreads";
  private static final String WEIGHT           = "weight";
  private static final String STARTINTERVAL    = "startInterval";
  private static final String ENDINTERVAL      = "endInterval";

  private static final String THREADGROUP      = "threadGroup";
  private static final String THREADGROUPS     = "threadGroups";

  private static final String TOTALTHREADS     = "totalThreads";
  private static final String TOTALVMS         = "totalVMs";

  private static final String ASSIGNMENT       = "assignment";

  private static final String FCN              = "fcn";
  private static final String NCF              = "ncf";

  private static final String ONEOF            = "oneof";
  private static final String FOENO            = "foeno";
  private static final String ROBING           = "robing";
  private static final String GNIBOR           = "gnibor";
  private static final String RANGE            = "range";
  private static final String EGNAR            = "egnar";

  private static final String TRUE             = "true";
  private static final String FALSE            = "false";

  private static final String DASH             = "-";
  private static final String EQUALS           = "=";
  private static final String PLUSEQUALS       = "+=";
  private static final String COMMA            = ",";
  private static final String SEMI             = ";";
  private static final String SPACE            = " ";
  private static final String NEWLINE          = "\n";

  private static List Nonassignments = new ArrayList();

  //////////////////////  Constructors  //////////////////////

  /**
   * Creates a new <code>ConfigParser</code>
   */
  private ConfigParser() {
  }

  //// RULE: config = (stmt SEMI)*
  private void parse( TestConfig tc, Vector tokens ) {
    Vector stmt = new Vector();
    for ( ListIterator i = tokens.listIterator(); i.hasNext(); ) {
      String token;
      while ( i.hasNext() ) {
        token = (String) i.next();
        if (token.equals(SEMI)) {
          if ( stmt.size() > 0 ) {
            try {
              //log().finest( "Parsing:" + reconstruct( stmt ) );
              String type = parseStmt( tc, stmt );
              if ( type.equals( ASSIGNMENT ) ) {
                // do nothing
              } else {
                String s = reconstruct(stmt);
                Nonassignments.add( s );
              }
              //log().finest( "Parsed:" + reconstruct( stmt ) );
            } catch( HydraConfigException e ) {
              e.printStackTrace();
              throw new HydraConfigException( "Malformed statement:" + reconstruct( stmt ), e );
            }
            stmt.setSize( 0 );
            break;
          } // else ignore extraneous semicolon
        } else {
          stmt.add( token );
        }
      }
    }
    if ( stmt.size() > 0 ) {
      throw new HydraConfigException( "Missing semicolon delimiter (;) at end of statement:" + reconstructMostly( stmt ) );
    }
  }
  //// RULE: stmt = task | threadgroup | assignment
  private String parseStmt( TestConfig tc, Vector stmt ) {
    String token = (String) stmt.elementAt(0);
    if ( token.equalsIgnoreCase( UNITTEST ) ) {
      parseUnitTest( tc, stmt );
      return UNITTEST;
    } else if ( token.equalsIgnoreCase( STARTTASK ) ) {
      parseStartTask( tc, stmt );
      return STARTTASK;
    } else if ( token.equalsIgnoreCase( INITTASK ) ) {
      parseInitTask( tc, stmt );
      return INITTASK;
    } else if ( token.equalsIgnoreCase( TASK ) ) {
      parseTask( tc, stmt );
      return TASK;
    } else if ( token.equalsIgnoreCase( CLOSETASK ) ) {
      parseCloseTask( tc, stmt );
      return CLOSETASK;
    } else if ( token.equalsIgnoreCase( ENDTASK ) ) {
      parseEndTask( tc, stmt );
      return ENDTASK;
    } else if ( token.equalsIgnoreCase( THREADGROUP ) )  {
      parseThreadGroup( tc, stmt );
      return THREADGROUP;
    } else {
      parseAssignment( tc, stmt );
      return ASSIGNMENT;
    }
  }

  private TestTask parseUnitTest( Vector stmt ) {
    JUnitTestTask tt = new JUnitTestTask();
    ListIterator i = stmt.listIterator();
    i.next(); // move past task keyword
    while ( i.hasNext() ) {
      String token = (String) i.next();
      if ( token.equalsIgnoreCase( TESTCLASS ) )
        parseTestClass( i, tt );
      else if ( token.equalsIgnoreCase( TESTMETHOD ) )
        parseTestMethod( i, tt );
      else
        throw new HydraConfigException( "Unexpected token: " + token );
    }

    if ( tt.getTestClasses().size() <= 0 )
      throw new HydraConfigException( "Missing " + TESTCLASS  );
    return tt;
  }

  private TestTask parseStartAndEndTask( TestConfig tc, Vector stmt ) {
    TestTask tt = new TestTask();
    ListIterator i = stmt.listIterator();
    i.next(); // move past task keyword
//    boolean ctask = false;
    while ( i.hasNext() ) {
      String token = (String) i.next();
      if ( token.equalsIgnoreCase( TASKCLASS ) ) {
        parseTaskClass( i, tt );
      } else if ( token.equalsIgnoreCase( TASKMETHOD ) ) {
        parseTaskMethod( i, tt );
      } else if ( token.equalsIgnoreCase( CLIENTNAMES ) ) {
        parseClientNames( i, tt );
      } else if ( token.equalsIgnoreCase( BATCH ) ) {
        parseBatch( tt );
      } else {
        i.previous();
        parseTaskAttribute( tc, i, tt );
      }
    }
    checkTarget( tt );
    return tt;
  }
  private TestTask parseInitAndCloseTask( TestConfig tc, Vector stmt ) {
    TestTask tt = new TestTask();
    ListIterator i = stmt.listIterator();
    i.next(); // move past task keyword
    while ( i.hasNext() ) {
      String token = (String) i.next();
      if ( token.equalsIgnoreCase( TASKCLASS ) ) {
        parseTaskClass( i, tt );
      } else if ( token.equalsIgnoreCase( TASKMETHOD ) ) {
        parseTaskMethod( i, tt );
      } else if ( token.equalsIgnoreCase( THREADGROUPS ) ) {
        parseThreadGroupNames( i, tt );
      } else if ( token.equalsIgnoreCase( SEQUENTIAL ) ) {
        parseSequential( tt );
      } else if ( token.equalsIgnoreCase( BATCH ) ) {
        parseBatch( tt );
      } else if ( token.equalsIgnoreCase( RUNMODE ) ) {
        parseRunMode( i, tt );
      } else {
        i.previous();
        parseTaskAttribute( tc, i, tt );
      }
    }
    checkTarget( tt );
    return tt;
  }
  private TestTask parseComplexTask( TestConfig tc, Vector stmt ) {
    TestTask tt = new TestTask();
    ListIterator i = stmt.listIterator();
    i.next(); // move past task keyword
    while ( i.hasNext() ) {
      String token = (String) i.next();
      if ( token.equalsIgnoreCase( TASKCLASS ) ) {
        parseTaskClass( i, tt );
      } else if ( token.equalsIgnoreCase( TASKMETHOD ) ) {
        parseTaskMethod( i, tt );
      } else if ( token.equalsIgnoreCase( THREADGROUPS ) ) {
        parseThreadGroupNames( i, tt );
      } else if ( token.equalsIgnoreCase( MAXTIMESTORUN ) ) {
        parseMaxTimesToRun( i, tt );
      } else if ( token.equalsIgnoreCase( MAXTHREADS ) ) {
        parseMaxThreads( i, tt );
      } else if ( token.equalsIgnoreCase( WEIGHT ) ) {
        parseWeight( i, tt );
      } else if ( token.equalsIgnoreCase( STARTINTERVAL ) ) {
        parseStartInterval( i, tt );
      } else if ( token.equalsIgnoreCase( ENDINTERVAL ) ) {
        parseEndInterval( i, tt );
      } else {
        i.previous();
        parseTaskAttribute( tc, i, tt );
      }
    }
    checkTarget( tt );
    return tt;
  }
  private void checkTarget( TestTask tt ) {
    if ( tt.getReceiver() == null ) {
      throw new HydraConfigException( "Missing " + TASKCLASS  );
    }
    if ( tt.getSelector() == null ) {
      throw new HydraConfigException( "Missing " + TASKMETHOD  );
    }
  }
  private void parseTaskClass( ListIterator i, TestTask tt ) {
    if ( tt.getReceiver() == null ) {
      parseEquals( i );
      String jid = parseJavaIdentifier( i );
      tt.setReceiver( jid );
    } else {
      throw new HydraConfigException( "Attempt to set " + TASKCLASS + " more than once" );
    }
  }
  private void parseTaskMethod( ListIterator i, TestTask tt ) {
    if ( tt.getSelector() == null ) {
      parseEquals( i );
      String jid = parseJavaIdentifier( i );
      tt.setSelector( jid );
    } else {
      throw new HydraConfigException( "Attempt to set " + TASKMETHOD + "  more than once" );
    }
  }

  private void parseTestClass( ListIterator i, JUnitTestTask tt ) {
    parseEquals( i );
    String jid = parseJavaIdentifier( i );
    tt.addTestClass( jid );
  }

  private void parseTestMethod( ListIterator i, JUnitTestTask tt ) {
    parseEquals( i );
    String jid = parseJavaIdentifier( i );
    tt.addTestMethod( jid );
  }

  private void parseSequential(TestTask tt) {
    tt.setSequential(true);
  }

  private void parseBatch( TestTask tt ) {
    tt.setBatch( true );
  }

  private void parseRunMode( ListIterator i, TestTask tt ) {
    if ( tt.getRunMode() == -1 ) {
      parseEquals( i );
      parseRunModeValue( i, tt );
    } else {
      throw new HydraConfigException( "Attempt to set " + RUNMODE + "  more than once" );
    }
  }

  private void parseRunModeValue( ListIterator i, TestTask tt ) {
    try {
      String token = (String) i.next();
      if ( token.equalsIgnoreCase( ALWAYS ) ) {
        tt.setRunMode( TestTask.ALWAYS );
      } else if ( token.equalsIgnoreCase( ONCE ) ) {
        tt.setRunMode( TestTask.ONCE );
      } else if ( token.equalsIgnoreCase( DYNAMIC ) ) {
        tt.setRunMode( TestTask.DYNAMIC );
      } else {
        throw new HydraConfigException( "Illegal value for " + RUNMODE );
      }
    } catch( NoSuchElementException e ) {
      throw new HydraConfigException( "Missing assigned value for " + RUNMODE );
    }
  }

  private void parseClientNames( ListIterator i, TestTask tt ) {
    if ( tt.getClientNames() == null ) {
      parseEquals( i );
      HydraVector names = parseUnterminatedCommaIdentifierList( i );
      if ( names.size() == 0 )
        throw new HydraConfigException( "Missing assigned value(s) for " + CLIENTNAMES );
      tt.setClientNames( names );
    } else {
      throw new HydraConfigException( "Attempt to set " + CLIENTNAMES + " more than once" );
    }
  }
  private void parseThreadGroupNames( ListIterator i, TestTask tt ) {
    if ( tt.getThreadGroupNames() == null ) {
      parseEquals( i );
      HydraVector names = parseUnterminatedCommaIdentifierList( i );
      if ( names.size() == 0 )
        throw new HydraConfigException( "Missing assigned value(s) for " + THREADGROUPS );
      tt.setThreadGroupNames( names );
    } else {
      throw new HydraConfigException( "Attempt to set " + THREADGROUPS + " more than once" );
    }
  }
  private HydraVector parseUnterminatedCommaIdentifierList( ListIterator i ) {
    // identifier (COMMA identifier)*
    HydraVector list = new HydraVector();

    // get the first one: identifier
    Object identifier = parseIdentifier( i );
    if ( identifier instanceof String ) {
      if ( ((String)identifier).equals( COMMA ) ) {
        throw new HydraConfigException( "Found comma instead of identifier" );
      } else { // assume real value
        list.add( identifier );
      }
    } else if ( identifier instanceof HydraVector ) {
      list.addAll( (HydraVector) identifier );
    } else {
      throw new HydraInternalException( "Should not happen" );
    }
    // get the rest: (COMMA identifier)*
    while ( i.hasNext() ) {
      identifier = parseIdentifier( i );
      if ( identifier instanceof String ) {
        if ( identifier.equals( COMMA ) ) { // look for real value
          identifier = parseIdentifier( i );
          if ( identifier.equals( COMMA ) ) {
            throw new HydraConfigException( "Extra comma" );
          } else { // assume real value
            list.add( identifier );
          }
        } else { // assume we're done if this isn't a comma
          i.previous();
          break;
        }
      } else if ( identifier instanceof HydraVector ) {
        list.addAll( (HydraVector) identifier );
      } else {
        throw new HydraInternalException( "Should not happen" );
      }
    }
    return list;
  }
  private void parseMaxTimesToRun( ListIterator i, TestTask tt ) {
    if ( tt.getMaxTimesToRun() == -1 ) {
      parseEquals( i );
      int n = parseInteger( i );
      tt.setMaxTimesToRun( n );
    } else {
      throw new HydraConfigException( "Attempt to set " + MAXTIMESTORUN + " more than once" );
    }
  }
  private void parseMaxThreads( ListIterator i, TestTask tt ) {
    if ( tt.getMaxThreads() == -1 ) {
      parseEquals( i );
      int n = parseInteger( i );
      tt.setMaxThreads( n );
    } else {
      throw new HydraConfigException( "Attempt to set " + MAXTHREADS + " more than once" );
    }
  }
  private void parseWeight( ListIterator i, TestTask tt ) {
    if ( tt.getWeight() == -1 ) {
      parseEquals( i );
      int n = parseInteger( i );
      tt.setWeight( n );
    } else {
      throw new HydraConfigException( "Attempt to set " + WEIGHT + "  more than once" );
    }
  }
  private void parseStartInterval( ListIterator i, TestTask tt ) {
    if ( tt.getStartInterval() == -1 ) {
      parseEquals( i );
      int n = parseInteger( i );
      tt.setStartInterval( n );
    } else {
      throw new HydraConfigException( "Attempt to set " + STARTINTERVAL + " more than once" );
    }
  }
  private void parseEndInterval( ListIterator i, TestTask tt ) {
    if ( tt.getEndInterval() == -1 ) {
      parseEquals( i );
      int n = parseInteger( i );
      tt.setEndInterval( n );
    } else {
      throw new HydraConfigException( "Attempt to set " + ENDINTERVAL + " more than once" );
    }
  }
  /** Returns true if the key is not allowed as a task attribute. */
  private boolean invalidTaskAttribute(String keystr) {
    return keystr.startsWith("hydra.AdminPrms")
        || keystr.startsWith("hydra.AgentPrms")
        || keystr.startsWith("hydra.BridgePrms")
        || keystr.startsWith("hydra.CachePrms")
        || keystr.startsWith("hydra.ClientCachePrms")
        || keystr.startsWith("hydra.PoolPrms")
        || keystr.startsWith("hydra.DiskStorePrms")
        || keystr.startsWith("hydra.GatewayReceiverPrms")
        || keystr.startsWith("hydra.GatewaySenderPrms")
        || keystr.startsWith("hydra.AsyncEventQueuePrms")
        || keystr.startsWith("hydra.GatewayPrms")
        || keystr.startsWith("hydra.GatewayHubPrms")
        || keystr.startsWith("hydra.FixedPartitionPrms")
        || keystr.startsWith("hydra.PartitionPrms")
        || keystr.startsWith("hydra.ResourceManagerPrms")
        || keystr.startsWith("hydra.SecurityPrms")
        || keystr.startsWith("hydra.RegionPrms")
        || keystr.startsWith("hydra.ClientRegionPrms")
        || keystr.startsWith("hydra.VersionPrms")
        ;
  }
  private void parseTaskAttribute( TestConfig tc, ListIterator i, TestTask tt ) {
    // key = value, where value is everything up to the word before the next =

    // do not allow certain keys to be used as task attributes
    String keystr = (String)i.next();
    if (invalidTaskAttribute(keystr)) {
      String s = "Illegal attempt to use as task attribute: " + keystr;
      throw new HydraConfigException(s);
    }
    i.previous();

    // get the key
    Long key = parseKey( tc, i );
    if ( tt.getTaskAttributes().get( key ) != null ) {
      throw new HydraConfigException( "Attempt to set task attribute " + key + " more than once" );
    }

    // deal with the equals
    parseEquals( i );

    // get the value
    ListIterator valuetokens = getNextAttributeValueIterator( i );
    if ( valuetokens == null ) {
      throw new HydraConfigException( "Missing value for task attribute " + key );
    }
    Object value = parseValue( valuetokens );
    tt.setTaskAttribute( key, value );
  }
  /** returns the subset of the list iterator that is the attribute value */
  private ListIterator getNextAttributeValueIterator( ListIterator i ) {
    Vector valuetokens = getNextAttributeValue( i );
    //log().finest( "getNextAttributeValueIterator returning " + valuetokens );
    return valuetokens.size() == 0 ? null : valuetokens.listIterator();
  }
  private Vector getNextAttributeValue( ListIterator i ) {
    Vector valuetokens = new Vector();
    if ( i.hasNext() ) {
      String valuetoken1 = (String) i.next();
      //log().finest( "getNextAttributeValue considering 1 " + valuetoken1 );
      if ( valuetoken1.equals( EQUALS ) || valuetoken1.equals( PLUSEQUALS ) ) {
        throw new HydraConfigException( "Task attribute value cannot begin with \"= or +=\"" );
      } else if ( i.hasNext() ) {
        String valuetoken2 = (String) i.next();
        //log().finest( "getNextAttributeValue considering 2 " + valuetoken2 );
        if ( valuetoken2.equals( PLUSEQUALS ) ) {
          throw new HydraConfigException( "Task attribute value cannot use \"+=\"" );
        } else if ( valuetoken2.equals( EQUALS ) ) {
          // found next attribute, so terminate before that
          i.previous(); // restore valuetoken2
          i.previous(); // restore valuetoken1
        } else {
          valuetokens.add( valuetoken1 );
          i.previous(); // restore valuetoken2
          //log().finest( "getNextAttributeValue recursing with " + valuetokens );
          valuetokens.addAll( getNextAttributeValue( i ) );
        }
      } else {
        valuetokens.add( valuetoken1 );
      }
    }
    //log().finest( "getNextAttributeValue returning " + valuetokens );
    return valuetokens;
  }
  // add support for list of identifiers (it is up to caller to cast it)
  private Object parseIdentifier( ListIterator i ) {
    // @todo lises check that this is a proper identifier (could start with $)
    try {
      String token = (String) i.next();
      if (token.equalsIgnoreCase(FCN)) {
        i.previous();
        parseFunction(i);
        return "unknown";
      } else if (token.indexOf("${") != -1) {
        return "unknown";
      } else {
        return token;
      }
    } catch( NoSuchElementException e ) {
      throw new HydraConfigException( "Missing identifier" );
    }
  }
  private String parseNumber( ListIterator i ) {
    // @todo lises check that this is a proper number
    try {
      String token = (String) i.next();
      if (token.equalsIgnoreCase(FCN)) {
        i.previous();
        parseFunction(i);
        return "999";
      } else if (token.indexOf("${") != -1) {
        return "999";
      } else {
        return token;
      }
    } catch( NoSuchElementException e ) {
      throw new HydraConfigException( "Missing numeric value" );
    }
  }
  private int parseInteger( ListIterator i ) {
    String token = null;
    try {
      token = (String) i.next();
      if (token.equalsIgnoreCase(FCN)) {
        i.previous();
        parseFunction(i);
        return 999;
      } else if (token.indexOf("${") != -1) {
        return 999;
      } else {
        return Integer.parseInt( token );
      }
    } catch( NumberFormatException e ) {
      String s = "Attempt to set value to non-integer: " + token;
      throw new HydraConfigException( s );
    } catch( NoSuchElementException e ) {
      throw new HydraConfigException( "Missing integer value" );
    }
  }
  private String parseJavaIdentifier( ListIterator i ) {
    // @todo lises check that this is a proper java identifier
    try {
      String token = (String) i.next();
      if (token.equalsIgnoreCase(FCN)) {
        i.previous();
        parseFunction(i);
        return "ignore";
      } else if (token.indexOf("${") != -1) {
        return "ignore";
      } else {
        return token;
      }
    } catch( NoSuchElementException e ) {
      throw new HydraConfigException( "Missing java identifier" );
    }
  }
  private void parseEquals( ListIterator i ) {
    try {
      String token = (String) i.next();
      if ( ! token.equals( EQUALS ) ) {
        throw new HydraConfigException( "Missing equals (=) operator, unexpected token: " + token );
      }
    } catch( NoSuchElementException e ) {
      throw new HydraConfigException( "Missing equals (=) operator" );
    }
  }
  private String parseEqualsOrPlusEquals( ListIterator i ) {
    try {
      String token = (String) i.next();
      if ( token.equals( EQUALS ) || token.equals( PLUSEQUALS ) ) {
        return token;
      } else {
        throw new HydraConfigException( "Missing equals (=) or (+=) operator, unexpected token: " + token );
      }
    } catch( NoSuchElementException e ) {
      throw new HydraConfigException( "Missing equals (=) or (+=) operator" );
    }
  }
  private void parseComma( ListIterator i ) {
    try {
      String token = (String) i.next();
      if ( ! token.equals( COMMA ) )
        throw new HydraConfigException( "Missing comma (,) delimiter, unexpected token: " + token );
    } catch( NoSuchElementException e ) {
      throw new HydraConfigException( "Missing comma (,) delimiter" );
    }
  }
  private void parseUnitTest( TestConfig tc, Vector stmt ) {
    TestTask tt = parseUnitTest( stmt );
    tt.setTaskType( TestTask.UNITTEST );
    tc.addUnitTest( tt );
  }
  private void parseStartTask( TestConfig tc, Vector stmt ) {
    TestTask tt = parseStartAndEndTask( tc, stmt );
    tt.setTaskType( TestTask.STARTTASK );
    tc.addStartTask( tt );
  }
  private void parseInitTask( TestConfig tc, Vector stmt ) {
    TestTask tt = parseInitAndCloseTask( tc, stmt );
    tt.setTaskType( TestTask.INITTASK );
    tc.addInitTask( tt );
  }
  private void parseTask( TestConfig tc, Vector stmt ) {
    TestTask tt = parseComplexTask( tc, stmt );
    tt.setTaskType( TestTask.TASK );
    tc.addTask( tt );
  }
  private void parseCloseTask( TestConfig tc, Vector stmt ) {
    TestTask tt = parseInitAndCloseTask( tc, stmt );
    tt.setTaskType( TestTask.CLOSETASK );
    tc.addCloseTask( tt );
  }
  private void parseEndTask( TestConfig tc, Vector stmt ) {
    TestTask tt = parseStartAndEndTask( tc, stmt );
    tt.setTaskType( TestTask.ENDTASK );
    tc.addEndTask( tt );
  }
  private void parseThreadGroup( TestConfig tc, Vector stmt ) {
    HydraThreadGroup tg = parseThreadGroup( stmt );
    if ( tg.getName().equalsIgnoreCase( HydraThreadGroup.DEFAULT_NAME ) )
      throw new HydraConfigException( "Cannot define reserved threadgroup \"" + HydraThreadGroup.DEFAULT_NAME + "\"" );
    tc.addThreadGroup( tg );
  }
  private HydraThreadGroup parseThreadGroup( Vector stmt ) {
    ListIterator i = stmt.listIterator();
    i.next(); // move past threadgroup keyword
    String name = parseJavaIdentifier( i );
    HydraThreadGroup tg = new HydraThreadGroup( name );
    parseThreadSubgroups( i, tg );
    return tg;
  }
  private void parseThreadSubgroups( ListIterator i, HydraThreadGroup tg ) {
    parseThreadSubgroup( i, tg ); // parse the first one
    while ( i.hasNext() ) {    // parse the rest, if any
      parseThreadSubgroup( i, tg );
    }
  }
  private void parseThreadSubgroup( ListIterator i, HydraThreadGroup tg ) {
    HydraThreadSubgroup tsg = new HydraThreadSubgroup( tg.getName() );
    parseTotalThreads( i, tsg );
    parseTotalVMs( i, tsg );
    parseClientNames( i, tsg );
    tg.addSubgroup( tsg );
  }
  private void parseTotalThreads( ListIterator i, HydraThreadSubgroup tsg ) {
    while ( i.hasNext() ) {
      String token = (String) i.next();
      if ( token.equalsIgnoreCase( TOTALTHREADS ) ) {
        parseEquals( i );
        int n = parseInteger( i );
        if ( n <= 0 )
          throw new HydraConfigException( "Illegal value for " + TOTALTHREADS );
        tsg.setTotalThreads( n );
        return;
      } else {
        throw new HydraConfigException( "Expected token: " + TOTALTHREADS + " Found: " + token );
      }
    }
    throw new HydraConfigException( "Missing " + TOTALTHREADS );
  }
  private void parseTotalVMs( ListIterator i, HydraThreadSubgroup tsg ) {
    while ( i.hasNext() ) {
      String token = (String) i.next();
      if ( token.equalsIgnoreCase( TOTALVMS ) ) {
        parseEquals( i );
        int n = parseInteger( i );
        if ( n <= 0 )
          throw new HydraConfigException( "Illegal value for " + TOTALVMS );
        tsg.setTotalVMs( n );
        return;
      } else {
        i.previous();
        tsg.setTotalVMs( 0 );
        return;
      }
    }
    // allow to default
    tsg.setTotalVMs( 0 );
  }
  private void parseClientNames( ListIterator i, HydraThreadSubgroup tsg ) {
    while ( i.hasNext() ) {
      String token = (String) i.next();
      if ( token.equalsIgnoreCase( CLIENTNAMES ) ) {
        parseEquals( i );
        HydraVector names = parseUnterminatedCommaIdentifierList( i );
        if ( names.size() == 0 )
          throw new HydraConfigException( "Missing assigned value(s) for " + CLIENTNAMES );
        tsg.setClientNames( names );
        return;
      } else {
        i.previous();
        tsg.setClientNames( null );
        return;
      }
    }
    // allow to default
    tsg.setClientNames( null );
  }
  private void parseAssignment( TestConfig tc, Vector stmt ) {
    ListIterator i = stmt.listIterator();
    Long key = parseKey( tc, i );
    String type = parseEqualsOrPlusEquals( i );
    Object value = parseValue( i );
    if ( type.equals( EQUALS ) ) {
      tc.addParameter( key, value );
    } else if ( type.equals( PLUSEQUALS ) ) {
      tc.addToParameter( key, value );
    } else {
      throw new HydraInternalException( "Should not happen" );
    }
  }
  private Long parseKey( TestConfig tc, ListIterator i ) {
    String fullname = (String) i.next();
    StringTokenizer st = new StringTokenizer( fullname, DASH, false );
    if ( st.countTokens() != 2 )
      throw new HydraConfigException( "Malformed key" );
    // get classname to pass to clients to force loading
    // of the class to run its static initializer so toString works
    String clsname = st.nextToken();
    tc.addClassName( clsname );
    return BasePrms.keyForName( fullname );
  }
  private Object parseValue( ListIterator i ) {
    //log().finest( "parseValue" );
    HydraVector listOfLists = new HydraVector();
    Object list = parseList( i );
    listOfLists.add( list );
    boolean vecvec = false;
    while ( i.hasNext() ) {
      list = parseCommaList( i );
      listOfLists.add( list );
      vecvec = true;
    }
    listOfLists = dealWithListValues( listOfLists );
    if ( listOfLists.size() == 0 ) {
      throw new HydraConfigException( "Malformed value" );
    } else if ( listOfLists.size() == 1 ) {
      return listOfLists.elementAt(0);
    } else {
      // true list of lists, reenlist elements as needed
      if ( vecvec ) {
        for ( int n = 0; n < listOfLists.size(); n++ ) {
          Object obj = listOfLists.elementAt(n);
          if ( ! ( obj instanceof HydraVector ) ) {
            listOfLists.setElementAt( new HydraVector(obj), n );
          }
        }
      }
      return listOfLists;
    }
  }
  private HydraVector dealWithListValues( HydraVector list ) {
    HydraVector newList = new HydraVector();
    for ( Iterator listelts = list.iterator(); listelts.hasNext(); ) {
      Object o = listelts.next();
      if ( o instanceof HydraVector ) {
        newList.add( dealWithListValues( (HydraVector)o ) );
      } else {
        newList.add( o );
      }
    }
    return newList;
  }
  /** returns elements up to the next comma as a hydravector of elements or,
      if there is only one element, a simple value */
  private Object parseList( ListIterator i ) {
    //log().finest( "parseList" );
    HydraVector list = new HydraVector();
    while ( i.hasNext() ) {
      Object listelement = parseListElement( i );
      if ( listelement.equals( EQUALS ) || listelement.equals( PLUSEQUALS ) ) {
        throw new HydraConfigException( "Found delimiter \"= or +=\" in assigned value.  To include delimiters as values, use double quotes." );
      } else if ( listelement.equals( COMMA ) ) {
        i.previous();
        break;
      } else {
        //log().finest( "adding " + listelement + " to list" );
        list.add( listelement );
      }
    }
    if ( list.size() == 0 )
      throw new HydraConfigException( "Missing assigned value(s)" );
    return ( list.size() == 1 ) ? list.elementAt(0) : list;
  }
  private Object parseCommaList( ListIterator i ) {
    //log().finest( "parseCommaList" );
    parseComma( i );
    return parseList( i );
  }
  private Object parseListElement( ListIterator i ) {
    //log().finest( "parseListElement");
    String token = (String) i.next();
    if (token.equalsIgnoreCase(RANGE)) {
      return parseRange(i);
    } else if (token.equalsIgnoreCase(ONEOF)) {
      return parseOneOf(i);
    } else if (token.equalsIgnoreCase(ROBING)) {
      return parseRobinG(i);
    } else if (token.equalsIgnoreCase(FCN)) {
      i.previous();
      parseFunction(i);
      return "ignore";
    } else if (token.indexOf("${") != -1) {
      return "ignore";
    } else { // @todo lises be more specific here
      i.previous();
      return (String) parseIdentifier(i);
    }
  }

  private Range parseRange( ListIterator i ) {
    double lo, hi;
    String lower = parseNumber( i );
    String upper = parseNumber( i );
    try {
      lo = Double.parseDouble( lower );
    } catch( NumberFormatException e ) {
      String s = "RANGE contains non-numeric value: " + lower;
      throw new HydraConfigException( s );
    }
    try {
      hi = Double.parseDouble( upper );
    } catch( NumberFormatException e ) {
      String s = "RANGE contains non-numeric value: " + upper;
      throw new HydraConfigException( s );
    }
    try {
      String token = (String) i.next();
      if ( ! token.equalsIgnoreCase( EGNAR ) )
        throw new HydraConfigException( "Unexpected token in RANGE (expected EGNAR): " + token );
    } catch( NoSuchElementException e ) {
      throw new HydraConfigException( "Missing EGNAR terminator at end of RANGE" );
    }
    return new Range( lo, hi );
  }
  private OneOf parseOneOf( ListIterator i ) {
    //log().finest( "parseOneOf" );
    Vector vals = new Vector();
    while ( i.hasNext() ) {
      String token = (String) i.next();
      //log().finest( "parseOneOf: " + token );
      if ( token.equalsIgnoreCase( FOENO ) ) {
        //log().finest( "parseOneOf: done" );
        break;
      } else if ( token.equalsIgnoreCase( RANGE ) ) {
        vals.add( parseRange( i ) );
      } else if ( token.equalsIgnoreCase( ONEOF ) ) {
        vals.add( parseOneOf( i ) );
      } else if ( token.equalsIgnoreCase( FCN ) ) {
        i.previous();
        parseFunction(i);
        vals.add("ignore");
      } else if (token.indexOf("${") != -1) {
        vals.add("ignore");
      } else {
        //log().finest( "parseOneOf: adding " + token );
        vals.add( token );
      }
    }
    if ( vals.size() > 0 ) {
      return new OneOf( vals );
    } else {
      throw new HydraConfigException( "ONEOF contains no values" );
    }
  }
  private RobinG parseRobinG( ListIterator i ) {
    //log().finest( "parseRobinG" );
    Vector vals = new Vector();
    while ( i.hasNext() ) {
      String token = (String) i.next();
      //log().finest( "parseRobinG: " + token );
      if ( token.equalsIgnoreCase( GNIBOR ) ) {
        //log().finest( "parseRobinG: done" );
        break;
      } else if ( token.equalsIgnoreCase( RANGE ) ) {
        vals.add( parseRange( i ) );
      } else if ( token.equalsIgnoreCase( ONEOF ) ) {
        vals.add( parseOneOf( i ) );
      } else if ( token.equalsIgnoreCase( ROBING ) ) {
        vals.add( parseRobinG( i ) );
      } else if ( token.equalsIgnoreCase( FCN ) ) {
        i.previous();
        parseFunction(i);
        vals.add("ignore");
      } else if (token.indexOf("${") != -1) {
        vals.add("ignore");
      } else {
        //log().finest( "parseRobinG: adding " + token );
        vals.add( token );
      }
    }
    if ( vals.size() > 0 ) {
      return new RobinG( vals );
    } else {
      throw new HydraConfigException( "ROBING contains no values" );
    }
  }

  private String reconstruct( Vector stmt ) {
    return reconstructMostly( stmt ) + SEMI;
  }
  private String reconstructMostly( Vector stmt ) {
    StringBuffer buf = new StringBuffer(100);
    for ( ListIterator i = stmt.listIterator(); i.hasNext(); ) {
      String token = (String)i.next();
      if (token.indexOf(SPACE) != -1) { // something like "This is a test"
        buf.append("\"").append(token).append("\"");
      } else if (token.indexOf(EQUALS) != -1) {
        if (token.equals(EQUALS) || token.equals(PLUSEQUALS)) {
          buf.append(token); // delimiter "=" or "+="
        } else { // something like "-Dfrip=5"
          buf.append("\"").append(token).append("\"");
        }
      } else { // contains no special characters
        buf.append(token);
      }
      if (i.hasNext()) buf.append(SPACE);
    }
    return buf.toString();
  }

//------------------------------------------------------------------------------
// TOKENIZER
//------------------------------------------------------------------------------

  /**
   * Reads the full text of the given file into a string for later parsing.
   * Evaluates hydra configuration properties, expands functions, and
   * recursively reads include files.  Appends the "local.conf" file if asked.
   */
  private Vector tokenize(String fn, TestConfig tc, String localConf)
  throws FileNotFoundException {

    log().info("PASS 1a: getText: " + fn);
    String pass1 = getText(fn);
    if (localConf != null) {
      log().info("PASS 1b: getText: " + localConf);
      pass1 += getText(localConf);
    }
    //log().info("PASS 1: getText: " + fn + "\n" + pass1);

    log().info("PASS 2: getTokens: " + fn);
    Vector pass2 = ConfigLexer.tokenize(pass1);
    //log().info("PASS 2: getTokens: " + fn + "\n" + pass2);

    log().info("PASS 3: expandSystemProperties: " + fn);
    Vector pass3 = expandSystemProperties(tc, pass2);
    //log().info("PASS 3: expandSystemProperties: " + fn + "\n" + pass3);

    log().info("PASS 4: expandFunctions: " + fn);
    Vector pass4 = expandFunctions(tc, pass3);
    //log().info("PASS 4: expandFunctions: " + fn + "\n" + pass4);

    log().info("PASS 5: expandIncludes: " + fn);
    Vector pass5 = expandIncludes(tc, pass4);
    //log().info("PASS 5: expandIncludes: " + fn + "\n" + pass5);

    return pass5;
  }

//------------------------------------------------------------------------------
// TOKENIZER: PASS 1 (TEXT)
//------------------------------------------------------------------------------

  /**
   * Reads the file into a string.
   */
  private String getText( String fn ) throws FileNotFoundException {
    if (fn.indexOf("${") != -1) {
      return "";
    }
    try {
      return FileUtil.getText( fn );
    } catch( FileNotFoundException e ) {
      throw e;
    } catch( IOException e ) {
      String s = "Error reading hydra configuration file " + fn;
      throw new HydraRuntimeException(s);
    }
  }

//------------------------------------------------------------------------------
// TOKENIZER: PASS 3 (SYSTEM PROPERTIES)
//------------------------------------------------------------------------------

  /**
   * Expands system properties in the given tokens.  Does support nesting.
   */
  private Vector expandSystemProperties(TestConfig tc, Vector tokens) {
    Vector expandedTokens = new Vector();
    if (tokens != null) {
      for (ListIterator i = tokens.listIterator(); i.hasNext();) {
        String token = (String)i.next();
        expandedTokens.add(expandSystemProperties(tc, token));
      }
    }
    return expandedTokens;
  }

  private String expandSystemProperties( TestConfig tc, String token ) {
    StringBuffer buf = new StringBuffer();
    StringCharacterIterator i = new StringCharacterIterator( token );
    while ( i.current() != CharacterIterator.DONE ) {
      char c = i.current();
      switch( c ) {
        case '$':
          char cc = i.next();
          switch( cc ) {
            case '{':
              gobbleProperty( tc, i, buf, false );
              break;
            default:
              //log().finest( String.valueOf( c ) );
              buf.append( c );
              //log().finest( String.valueOf( cc ) );
              buf.append( cc );
              break;
          }
          break;
        default:
          if ( c != CharacterIterator.DONE ) {
            //log().finest( String.valueOf( c ) );
            buf.append( c );
          }
          break;
      }
      i.next();
    }
    return buf.toString();
  }

  /** invoked after seeing "${" */
  private String gobbleProperty(TestConfig tc, StringCharacterIterator i,
                                StringBuffer buf, boolean nested) {
    StringBuffer propKey = new StringBuffer();
    while ( i.current() != CharacterIterator.DONE ) {
      char c = i.next();
      switch( c ) {
        case '$':
          char cc = i.next();
          switch( cc ) {
            case '{':
              String nestedPropVal = gobbleProperty( tc, i, buf, true );
              propKey.append(nestedPropVal);
              break;
            default:
              //log().finest( String.valueOf( c ) );
              buf.append( c );
              //log().finest( String.valueOf( cc ) );
              buf.append( cc );
              break;
          }
          break;
        case '}':
          String propVal = System.getProperty( propKey.toString() );
          if (propVal == null) {
            String s = "No system property set for " + propKey;
            throw new HydraConfigException( s );
          } else {
            //log().finest( propVal );
            // note property here
            if (!nested) {
              buf.append( propVal );
            }
            tc.addSystemProperty( propKey.toString(), propVal );
            return propVal.toString();
          }
        default:
          if ( c != CharacterIterator.DONE ) {
            //log().finest( "PROPKEY: " + c );
            propKey.append( c );
          }
          break;
      }
    }
    throw new HydraConfigException( "Incomplete system property: " + propKey );
  }

//------------------------------------------------------------------------------
// TOKENIZER: PASS 4 (FUNCTIONS)
//------------------------------------------------------------------------------

  /**
   * Expands functions in the given tokens.  Does not support nesting.
   */
  private Vector expandFunctions(TestConfig tc, Vector tokens) {
    Vector expandedTokens = new Vector();
    if (tokens != null) {
      for (ListIterator i = tokens.listIterator(); i.hasNext();) {
        String token = (String)i.next();
        if (token.equalsIgnoreCase(FCN)) {
          i.previous();
          expandedTokens.addAll(expandFunction(i));
        } else {
          expandedTokens.add(token);
        }
      }
    }
    return expandedTokens;
  }

  /**
   * Expands a function into a string.
   */
  private Vector expandFunction(ListIterator i) {
    Vector tokens = parseFunction(i);
    String result = evaluateFunction(tokens);
    return ConfigLexer.tokenize(result);
  }

  /**
   * Parses a function into a list of tokens.
   */
  private Vector parseFunction(ListIterator i) {
    Vector tokens = new Vector();
    String token = (String)i.next();
    if (!token.equalsIgnoreCase(FCN)) {
      String s = "Expected the " + FCN + " keyword: " + token;
      throw new HydraInternalException(s);
    }
    while (i.hasNext()) {
      token = (String)i.next();
      if (token.equalsIgnoreCase(NCF)) {
        break;
      } else {
        tokens.add(token);
      }
    }
    if (tokens.size() == 0) {
      String s = "FCN is empty";
      throw new HydraConfigException(s);
    }
    return tokens;
  }

  /**
   * Evaluates a function into a string.
   */
  private String evaluateFunction(Vector tokens) {
    Fcn fcn = new Fcn(tokens);
    String fcnstr = fcn.toBeanshellString();
    Interpreter interpreter = new Interpreter();
    Object o;
    try {
      o = interpreter.eval(fcnstr);
    } catch (EvalError e) {
      String s = "Unable to evaluate beanshell function: " + fcnstr;
      throw new HydraConfigException(s, e);
    }
    String result = o.toString();
    return result;
  }

//------------------------------------------------------------------------------
// TOKENIZER: PASS 5 (INCLUDES)
//------------------------------------------------------------------------------

  /**
   * Expands includes in the given tokens.  Does support nesting.
   */
  private Vector expandIncludes(TestConfig tc, Vector tokens)
  throws FileNotFoundException {
    Vector expandedTokens = new Vector();
    if (tokens != null) {
      for (ListIterator i = tokens.listIterator(); i.hasNext();) {
        String token = (String)i.next();
        if (token.equalsIgnoreCase(INCLUDE)) {
          i.previous();
          if (token.indexOf("${") == -1) {
            expandedTokens.addAll(expandInclude(i, tc, true));
          } else {
            expandedTokens.add(INCLUDE);
            expandedTokens.add(parseInclude(i));
            expandedTokens.add(SEMI);
          }
        } else if (token.equalsIgnoreCase(RANDOMINCLUDE)
             && tc.getRandomSeed() != -1 // not pre-parsing
             && tc.getParameters().getRandGen().nextBoolean()) { // not skipping
          i.previous();
          if (token.indexOf("${") == -1) {
            expandedTokens.addAll(expandRandomInclude(i, tc, true));
          } else {
            expandedTokens.add(INCLUDE);
            expandedTokens.add(parseRandomInclude(i));
            expandedTokens.add(SEMI);
          }
        } else if (token.equalsIgnoreCase(RANDOMINCLUDE)) {
          i.previous();
          if (token.indexOf("${") == -1) {
            expandRandomInclude(i, tc, true);
            // skipping over it here, do not add tokens
          }
        } else if (token.equalsIgnoreCase(INCLUDEIFPRESENT)) {
          i.previous();
          if (token.indexOf("${") == -1) {
            expandedTokens.addAll(expandInclude(i, tc, false));
          } else {
            expandedTokens.add(INCLUDE);
            expandedTokens.add(parseInclude(i));
            expandedTokens.add(SEMI);
          }
        } else {
          expandedTokens.add(token);
        }
      }
    }
    return expandedTokens;
  }

  /**
   * Expands an include or includeIfPresent into tokens.
   */
  private Vector expandInclude(ListIterator i, TestConfig tc, boolean required)
  throws FileNotFoundException {
    String fn = parseInclude(i);
    fn = EnvHelper.expandEnvVars(fn,
         tc.getMasterDescription().getVmDescription().getHostDescription());
    try {
      return tokenize(fn, tc, null);
    } catch (FileNotFoundException e) {
      if (required) { // include
        throw e;
      } else { // includeIfPresent
        return new Vector();
      }
    }
  }

  /**
   * Expands a randomInclude into tokens.
   */
  private Vector expandRandomInclude(ListIterator i, TestConfig tc, boolean required)
  throws FileNotFoundException {
    String fn = parseRandomInclude(i);
    fn = EnvHelper.expandEnvVars(fn,
         tc.getMasterDescription().getVmDescription().getHostDescription());
    try {
      return tokenize(fn, tc, null);
    } catch (FileNotFoundException e) {
      if (required) { // randomInclude
        throw e;
      } else { // includeIfPresent
        return new Vector();
      }
    }
  }

  /**
   * Parses an include into an unexpanded filename.
   */
  private String parseInclude(ListIterator i) {
    Vector tokens = new Vector();
    String token = (String)i.next();
    if (!token.equalsIgnoreCase(INCLUDE)) {
      String s = "Expected the " + INCLUDE + " keyword: " + token;
      throw new HydraInternalException(s);
    }
    while (i.hasNext()) {
      token = (String)i.next();
      if (token.equals(SEMI) ) {
        break;
      } else {
        tokens.add(token);
      }
    }
    if (tokens.size() == 0) {
      String s = "INCLUDE is empty";
      throw new HydraConfigException(s);
    } else if (tokens.size() == 1) {
      return (String)tokens.get(0);
    } else {
      String s = "Missing ; at end of statement: " + INCLUDE + tokens.get(0);
      throw new HydraConfigException(s);
    }
  }

  /**
   * Parses a randomInclude into an unexpanded filename.
   */
  private String parseRandomInclude(ListIterator i) {
    Vector tokens = new Vector();
    String token = (String)i.next();
    if (!token.equalsIgnoreCase(RANDOMINCLUDE)) {
      String s = "Expected the " + RANDOMINCLUDE + " keyword: " + token;
      throw new HydraInternalException(s);
    }
    while (i.hasNext()) {
      token = (String)i.next();
      if (token.equals(SEMI) ) {
        break;
      } else {
        tokens.add(token);
      }
    }
    if (tokens.size() == 0) {
      String s = "RANDOMINCLUDE is empty";
      throw new HydraConfigException(s);
    } else if (tokens.size() == 1) {
      return (String)tokens.get(0);
    } else {
      String s = "Missing ; at end of statement: " + RANDOMINCLUDE + tokens.get(0);
      throw new HydraConfigException(s);
    }
  }

//------------------------------------------------------------------------------
// STATIC METHODS
//------------------------------------------------------------------------------

  /**
   * Returns the log writer.
   */
  private static LogWriter log() {
    return Log.getLogWriter();
  }

  /**
   * Resets the parser for a fresh parse.
   */
  protected static void reset() {
    Nonassignments = new ArrayList();
  }

  /**
   * Writes the parsed assignment and other statements to the specified file.
   * Assignments are retrieved from the TestConfig to properly handle +=, and
   * all others come from the Nonassignments static variable.
   */
  private static void writeStmtsToFile(TestConfig tc, String fn) {
    StringBuffer buf = new StringBuffer();
    for ( Iterator i = Nonassignments.iterator(); i.hasNext(); ) {
      String stmt = (String) i.next();
      buf.append(stmt).append(NEWLINE);
    }
    Map assignments = TestConfig.tab().toSortedMap(true);
    for (Iterator i = assignments.keySet().iterator(); i.hasNext();) {
      String key = (String)i.next();
      String val = (String)assignments.get(key);
      String stmt = key + SPACE + EQUALS + SPACE + val + SEMI;
      buf.append(stmt).append(NEWLINE);
    }
    FileUtil.writeToFile( fn, buf.toString() );
  }

  /**
   * Parses the file into the test config, evaluating or not as ordered.
   */
  private static void parseFileMultiPass(String fn, TestConfig tc)
  throws FileNotFoundException {
    log().info("Parsing configuration file: " + fn);
    ConfigParser parser = new ConfigParser();
    String localConf = FileUtil.fileExists("local.conf") ? "local.conf" : null;
    Vector tokens = parser.tokenize(fn, tc, localConf);
    parser.parse(tc, tokens);
    log().info("Parsed configuration file: " + fn);
  }

  /**
   * Parses a hydra config file into a <code>TestConfig</code> object.
   *
   * @param fn fully-qualified name of the hydra config file
   * @param tc <code>TestConfig</code> to be populated from the config file
   * @param outfn fully-qualified name of the output file
   */
  public static void parseFile(String fn, TestConfig tc, String outfn)
  throws FileNotFoundException {
    parseFileMultiPass(fn, tc);
    if (outfn != null) {
      writeStmtsToFile(tc, outfn);
    }
  }

  /**
   * Parses the specified hydra config file.
   */
  private static void parseConfigFile(String configFileName, boolean print) {
    TestConfig tc = TestConfig.create();

    // set the random seed as soon as we can
    tc.setRandomSeed(tc.generateRandomSeed());

    // set master configuration
    MasterDescription.configure( tc );

    // read properties, if any
    int index = configFileName.indexOf( ".conf" );
    if ( index != -1 ) { // hydra test
      String propFileName = configFileName.substring(0, index) + ".prop";
      try {
        FileInputStream fis = new FileInputStream(propFileName);
        Properties p = new Properties();
        p.load(fis);
        for (Enumeration names = p.propertyNames(); names.hasMoreElements();) {
          String name = (String)names.nextElement();
          System.setProperty(name, p.getProperty(name));
        }
      } catch( FileNotFoundException e ) {
        // no property file provided
      } catch (IOException e) {
        String s = "Unable to load test properties from " + propFileName;
        throw new HydraConfigException(s);
      }
    }
    log().info( "Parsing hydra configuration file: " + configFileName + "..." );
    try {
      ConfigParser.parseFile(configFileName, tc, "latest.conf");
    } catch( FileNotFoundException e ) {
      throw new HydraConfigException( "Hydra configuration file not found: " + configFileName, e );
    }

    // finish configuring the test
    HostDescription.configure( tc );
    HadoopDescription.configure( tc );
    JProbeDescription.configure( tc );
    SSLDescription.configure( tc );
    SecurityDescription.configure( tc );
    GemFireDescription.configure( tc );
    VmDescription.configure( tc );
    HostAgentDescription.configure( tc );
    AdminDescription.configure( tc );
    AgentDescription.configure( tc );
    ClientDescription.configure( tc );
    DiskStoreDescription.configure( tc );
    BridgeDescription.configure( tc );
    FixedPartitionDescription.configure( tc );
    PartitionDescription.configure( tc );
    RegionDescription.configure( tc );
    ResourceManagerDescription.configure( tc );
    CacheDescription.configure( tc );
    GatewayReceiverDescription.configure( tc );
    GatewaySenderDescription.configure( tc );
    AsyncEventQueueDescription.configure( tc );
    GatewayDescription.configure( tc );
    GatewayHubDescription.configure( tc );

    // set the test name and runner
    String testDir = tc.getMasterDescription()
                       .getVmDescription().getHostDescription().getTestDir();
    if (configFileName.startsWith(testDir)) {
      tc.setTestName(configFileName.substring( testDir.length() + 1 ));

    } else {
      tc.setTestName(configFileName);
    }
    tc.setTestUser(System.getProperty("user.name"));

    // do the postprocessing step
    tc.postprocess();

    if (print) {
      System.out.println( tc );
    }

    // clean up the config
    TestConfig.destroy();
  }

  /**
   * Parses all hydra config files in $JTESTS and $EXTRA_JTESTS.
   */
  private static void parseConfigFiles() {
    log().info("Validating hydra configuration files...");
    List configFiles = TestFileUtil.getConfigFiles();
    for (Iterator i = configFiles.iterator(); i.hasNext();) {
      File configFile = (File)i.next();
      try {
        log().info("Validating " + configFile);
        ConfigParser.parseConfigFile(configFile.toString(), false /* print */);
      } 
      catch (VirtualMachineError e) {
        // Don't try to handle this; let thread group catch it.
        throw e;
      }
      catch (Throwable e) {
        String s = "While parsing \"" + configFile + "\": " + e;
        throw new HydraConfigException(s, e);
      }
    }
    log().info("Done validating hydra configuration files");
  }

  /**
   * Parses either all hydra configuration files or the single file
   * given as an argument.
   */
  public static void main( String args[] ) {
    Log.createLogWriter( "parser", "fine" );
    if (args.length == 0) {
      parseConfigFiles(); // parse all .conf files
    } else if (args.length == 1) {
      String configFileName = args[0];
      parseConfigFile(configFileName, true); // parse the given file only
    } else {
      throw new HydraRuntimeException("Usage: hydra.ConfigParser [configFile]");
    }
  }
}
