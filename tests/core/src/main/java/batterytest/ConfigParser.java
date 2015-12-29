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

package batterytest;

import hydra.*;

import java.io.*;
import java.util.*;

/** 
*
* Parses batterytest input files.
*
*/

public class ConfigParser {

  private static final String INCLUDE = "include";
  private static final String NEWLINE = "\n";
  private static final String COMMA   = ",";
  private static int ComparisonKey = 0;

  public static Vector parseFile( String fn ) throws FileNotFoundException {
    return parseFile( fn, false );
  }
  public static Vector parseFile( String fn, boolean verbose ) throws FileNotFoundException {

    ConfigParser parser = new ConfigParser();

    // read file contents
    String text = parser.getText( fn );

    // get tokens
    ConfigLexer lex = new ConfigLexer();
    Vector tokens = ConfigLexer.tokenize( text );
    if ( tokens.size() == 0 ) {
      System.out.println( "Batterytest input file " + fn + " is empty" );
      System.exit(0);
    }
    if ( verbose ) {
      System.out.println( "\nTokens..." );
      for ( Iterator i = tokens.iterator(); i.hasNext(); )
        System.out.println( i.next() );
    }

    // parse
    Vector finalTests = parser.parse( tokens, verbose );
    printTestFile( "batterytest.bt", finalTests );
    return finalTests;
  }
  //// RULE: config = (test (key=val(,val)*)* | include btfile )*
  private Vector parse( Vector tokens, boolean verbose ) {
    if ( verbose ) {
      System.out.println( "\n================" +
                          "\nSTARTING PARSE\n" + tokens +
                          "\n================\n" );
    }
    Vector tests = new Vector();
    int index = -1;
    ListIterator i = tokens.listIterator();
    while ( i.hasNext() ) {
      String token = (String) i.next();
      if ( verbose ) System.out.println( "TOKEN: " + token );
      if ( i.hasNext() ) {
        String nextToken = (String) i.next();
        if ( verbose ) System.out.println( "NEXT TOKEN: " + nextToken );
        if ( token.equalsIgnoreCase( INCLUDE ) ) {
          try {
            TestConfig tc;
            try {
              tc = TestConfig.create();
            } catch( HydraRuntimeException e ) {
              tc = TestConfig.getInstance();
            }

            MasterDescription.configure( tc );
            String fn = EnvHelper.expandEnvVars( nextToken,
                          tc.getMasterDescription().getVmDescription()
                            .getHostDescription() );
            Vector includedTests = parseFile( fn, verbose );
	    for ( Iterator it = includedTests.iterator(); it.hasNext(); ) {
	      BatteryTestConfig includedTest = (BatteryTestConfig) it.next();
              tests.add( ++index, includedTest );
	    }
          } catch( FileNotFoundException e ) {
            throw new HydraConfigException( "Include file not found: " + nextToken, e );
          }
        } else if ( nextToken.equals( "=" ) ) {
          if ( index == -1 ) {
            String s = "No test specified for system property: " + token;
            throw new HydraConfigException( s );
          } else if ( containsWhiteSpace( token ) ) {
            String s = "System property cannot include whitespace: " + token;
            throw new HydraConfigException( s );
          } else {
            if ( verbose ) System.out.println( "PARSING SYSPROP VALUE" );
            parseSysPropValue( i, tests, index, token, verbose );
          }
        } else {
          if ( verbose && index > -1 ) System.out.println( index + ":" + tests.elementAt(index) );
          if ( isTest( token ) ) {
            tests.add( ++index, new BatteryTestConfig( token ) );
            i.previous();
          }
        }
      } else {
        if ( verbose && index > -1 ) System.out.println( index + ":" + tests.elementAt(index) );
        if ( isTest( token ) ) {
          tests.add( ++index, new BatteryTestConfig( token ) );
        }
      }
    }
    return postprocess( tests, verbose );
  }
  private void parseSysPropValue( ListIterator i, Vector tests, int index,
                                  String sysprop, boolean verbose ) {
    if ( i.hasNext() ) {
      HydraVector values =
          parseUnterminatedCommaIdentifierList( i, verbose );
      if ( verbose ) System.out.println( "SYSPROP VALUES: " + values );
      BatteryTestConfig btc = (BatteryTestConfig) tests.get( index );
      if ( verbose ) System.out.println( "ADDING PROPERTY " + sysprop );
      btc.addProperty( sysprop, values );

    } else {
      String s = "Expected value for system property " + sysprop;
      throw new HydraConfigException( s );
    }
  }
  private HydraVector parseUnterminatedCommaIdentifierList( ListIterator i,
                                                            boolean verbose ) {
    // identifier (COMMA identifier)*
    HydraVector list = new HydraVector();
    
    // get the first one: identifier
    String identifier = parseValue( i );
    if ( verbose ) System.out.println( "COMMALIST TOKEN: " + identifier );
    if ( identifier.equals( COMMA ) ) {
      throw new HydraConfigException( "Found comma instead of identifier" );
    } else { // assume real value
      list.add( identifier );
    }
    // get the rest: (COMMA identifier)*
    while ( i.hasNext() ) { 
      identifier = parseValue( i );
      if ( verbose ) System.out.println( "COMMALIST TOKEN: " + identifier );
      if ( identifier.equals( COMMA ) ) { // look for real value
        identifier = parseValue( i );
        if ( verbose ) System.out.println( "COMMALIST TOKEN: " + identifier );
        if ( identifier.equals( COMMA ) ) {
          throw new HydraConfigException( "Extra comma" );
        } else { // assume real value
          list.add( identifier );
        }
      } else { // assume we're done if this isn't a comma
        i.previous();
        break;
      }
    }
    return list;
  }
  private String parseValue( ListIterator i ) {
    String token = (String) i.next();
    if ( containsWhiteSpace( token ) ) {
      String s = "System property cannot include whitespace: " + token;
      throw new HydraConfigException( s );
    }
    return token;
  }
  private boolean isTest( String token ) {
    if (token.indexOf(".conf") == -1 && // hydra
        token.indexOf(".pl") == -1 &&   // perl
        token.indexOf(".xml") == -1) {  // native client
      // @todo lises proscribe suffix(es) for all other possible test types and handle these here
      throw new HydraConfigException( "Expected .conf or .xml or .pl file, found: " + token );
    } else {
      return true;
    }
  }
  private boolean containsWhiteSpace( String token ) {
    return token.indexOf( ' ' ) > -1
       || token.indexOf( '\t' ) > -1
       || token.indexOf( '\n' ) > -1
       || token.indexOf( '\r' ) > -1
       || token.indexOf( '\f' ) > -1;
  }
  private String getText( String fn ) throws FileNotFoundException {
    StringBuffer buf = new StringBuffer(1000);
    BufferedReader br = new BufferedReader( new FileReader( fn ) );
    try {
      String s;
      while ( ( s = br.readLine() ) != null ) {
        buf.append( s );
        buf.append( NEWLINE );
      }
      br.close();
    } catch( IOException e ) {
      throw new HydraRuntimeException( "Error reading hydra configuration file " + fn, e );
    }
    return buf.toString();
  }
  private Vector postprocess( Vector tests, boolean verbose ) {
    Vector newTests = new Vector();
    for ( Iterator i = tests.iterator(); i.hasNext(); ) {
      BatteryTestConfig btc = (BatteryTestConfig) i.next();
      Vector btcp = btc.getProperties();
      if ( btc.postprocessed() || btcp == null ) {
        newTests.add( btc );
      } else {
        Vector properties = multiply( btcp.iterator() );
        if ( verbose ) System.out.println( "MULTIPLIER RESULT: " + properties );
        String name = btc.getName();
        for ( Iterator j = properties.iterator(); j.hasNext(); ) {
          BatteryTestConfig b = new BatteryTestConfig( name );
          b.setProperties( (Vector) j.next() );
          if ( verbose ) System.out.println( "CREATING TEST WITH PROPERTIES: " + b.getProperties() );
          String key = "perffmwk.comparisonKey";
          BatteryTestConfig.Property p = b.getProperty( key );
          if ( p != null ) {
            if ( ((String)p.getVal()).equalsIgnoreCase( "autogenerate" ) ) {
              b.resetProperty( key, String.valueOf( ComparisonKey ) );
              if ( verbose ) System.out.println( "RESET " + b.getProperty( key ) );
              ++ComparisonKey;
            } else {
              String val = (String)p.getVal();
              if (val.toLowerCase().startsWith("fromprops")) {
                b.resetProperty(key, b.getPropertyKeyString(val));
                if (verbose) System.out.println("RESET " + b.getProperty(key));
              }
            }
          }
          String onlyOnPlatforms =
            (String)b.getPropertyVal(HostHelper.ONLY_ON_PLATFORMS_PROP);
          if (HostHelper.shouldRun(onlyOnPlatforms)) {
            b.postprocess();
            newTests.add(b);
          } else if (verbose) {
            System.out.println("Skipping "
                  + b + " because it is only run on " + onlyOnPlatforms
                  + " and this platform is " + HostHelper.getLocalHostOS());
          }
        }
      }
    }
    if ( verbose ) System.out.println( "Generated " + newTests.size() + " tests from:\n" + tests + ":\n" + newTests );
    return newTests;
  }
  private static Vector multiply( Vector properties ) {
    return multiply( properties.iterator() );
  }
  private static Vector multiply( Iterator properties ) {
    BatteryTestConfig.Property property = (BatteryTestConfig.Property) properties.next();
    String key = property.getKey();
    if ( properties.hasNext() ) {
      Vector bodies = new Vector();
      Vector tails = multiply( properties );
      for ( Iterator i = ((Vector)property.getVal()).iterator(); i.hasNext(); ) {
        String head = (String) i.next();
        for ( Iterator j = tails.iterator(); j.hasNext(); ) {
          Vector tail = (Vector) j.next();
          Vector body = new Vector();
          body.add( new BatteryTestConfig.Property( key, head ) );
          for ( Iterator k = tail.iterator(); k.hasNext(); ) {
            BatteryTestConfig.Property p = (BatteryTestConfig.Property) k.next();
            body.add( p.copy() );
          }
          bodies.add( body );
        }
      }
      return bodies;
    } else { // last one
      Vector bodies = new Vector();
      for ( Iterator i = ((Vector)property.getVal()).iterator(); i.hasNext(); ) {
        String head = (String) i.next();
        Vector body = new Vector();
        body.add( new BatteryTestConfig.Property( key, head ) );
        bodies.add( body );
      }
      return bodies;
    }
  }
  private static void printTestFile( String fn, Vector tests ) {
    if ( tests == null ) return;
    StringBuffer buf = new StringBuffer();
    for ( Iterator i = tests.iterator(); i.hasNext(); ) {
      BatteryTestConfig btc = (BatteryTestConfig) i.next();
      buf.append( btc.toTestString() );
    }
    FileUtil.writeToFile( fn, buf.toString() );
  }
  public static void main( String args[] ) {
    String fn = args[0];
    try {
      Vector tests = ConfigParser.parseFile( fn, true );
      System.out.println( "\n================================================================================\n" );
      System.out.println( "FINAL LIST" );
      System.out.println( "\n================================================================================\n" );
      for ( Iterator i = tests.iterator(); i.hasNext(); ) {
        BatteryTestConfig btc = (BatteryTestConfig) i.next();
        System.out.println( btc );
      }
    } catch( FileNotFoundException e ) {
      System.out.println( "Batterytest input file " + fn + " not found." );
      e.printStackTrace();
      System.exit(1);
    }
  }
}
