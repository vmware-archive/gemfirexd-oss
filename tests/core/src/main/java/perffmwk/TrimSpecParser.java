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

package perffmwk;

import com.gemstone.gemfire.LogWriter;

import hydra.*;

import java.io.*;
import java.util.*;

/** 
*
* Parses trim specification files.
*
*/

public class TrimSpecParser {

  public static void parseFile( String fn, StatConfig statconfig ) throws FileNotFoundException {
    parseFile( fn, statconfig, false );
  }

  public static void parseFile( String fn, StatConfig statconfig, boolean verbose ) throws FileNotFoundException {

    TrimSpecParser parser = new TrimSpecParser();

    // read file contents
    if (verbose) log().info( "\n/////  BEGIN " + fn );
    String text = parser.getText( fn );
    if (verbose) log().info( "\n" + text );

    // get tokens
    ConfigLexer lex = new ConfigLexer();
    Vector tokens = ConfigLexer.tokenize( text );
    if ( tokens == null ) {
      log().warning( "Trim specification file " + fn + " is empty" );
      return;
    }
    //for ( ListIterator i = tokens.listIterator(); i.hasNext(); )
    //  log().info( i.next() );
    //log().info( tokens );

    // parse
    parser.parse( statconfig, tokens );

    if (verbose) log().info( "\n/////  END   " + fn );
  }
  //// RULE: config = (stmt TrimSpecTokens.SEMI)*
  private void parse( StatConfig statconfig, Vector tokens ) {
    Vector stmt = new Vector();
    for ( ListIterator i = tokens.listIterator(); i.hasNext(); ) {
      String token;
      while ( i.hasNext() ) {
        token = (String) i.next();
	if ( token.equals( ";" ) ) {
	  if ( stmt.size() > 0 ) {
	    try {
	      //log().finest( "Parsing:" + reconstruct( stmt ) );
	      parseStmt( statconfig, stmt );
	      //log().finest( "Parsed:" + reconstruct( stmt ) );
	    } catch( StatConfigException e ) {
	      e.printStackTrace();
	      throw new StatConfigException( "Malformed statement:" + reconstruct( stmt ), e );
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
      throw new StatConfigException( "Missing semicolon delimiter (;) at end of statement:" + reconstructMostly( stmt ) );
    }
  }
  //// RULE: stmt = include | task | threadgroup | assignment
  private void parseStmt( StatConfig statconfig, Vector stmt ) {
    String token = (String) stmt.elementAt(0);
    if ( token.equalsIgnoreCase( TrimSpecTokens.TRIMSPEC ) ) 
      parseTrimSpec( statconfig, stmt );
    else
      throw new StatConfigException( "Unknown statement type" );
  }
  private void parseTrimSpec( StatConfig statconfig, Vector stmt ) {
    TrimSpec trimspec = parseTrimSpec( stmt );
    statconfig.addTrimSpec( trimspec );
  }
  private TrimSpec parseTrimSpec( Vector stmt ) {
    ListIterator i = stmt.listIterator();
    i.next(); // move past trimspec keyword
    String name = parseJavaIdentifier( i );
    TrimSpec trimspec = new TrimSpec( name );
    parseTrimSpecParams( i, trimspec );
    return trimspec;
  }
  private void parseTrimSpecParams( ListIterator i, TrimSpec trimspec ) {
    while ( i.hasNext() ) {
      String token = (String) i.next();
      if (token.equalsIgnoreCase(TrimSpecTokens.START)) {
        String time = parseTime(i);
        Long timestamp = parseTimestamp(i);
        trimspec.setStart(time, timestamp);
      }
      else if (token.equalsIgnoreCase(TrimSpecTokens.END)) {
        String time = parseTime(i);
        Long timestamp = parseTimestamp(i);
        trimspec.setEnd(time, timestamp);
      }
      else {
        throw new StatConfigException("Unexpected token: " + token);
      }
    }
  }
  // 2003/09/25 17:21:43.701 PDT
  private String parseTime( ListIterator i ) {
    parseEquals( i );
    try {
      return (String) i.next() + " " + (String) i.next() + " " + (String) i.next();
    } catch( NoSuchElementException e ) {
      throw new StatConfigException( "Date is missing or incomplete" );
    }
  }
  // optional (1244737903988)
  private Long parseTimestamp(ListIterator i) {
    try {
      String token = (String)i.next();
      if (token.startsWith(TrimSpecTokens.LPAREN) &&
          token.endsWith(TrimSpecTokens.RPAREN)) {
        String timestamp = token.substring(1, token.length() - 1);
        return parseLong(timestamp);
      } else {
        // no timestamp, ok since must be backwards compatible
        i.previous();
        return null;
      }
    } catch( NoSuchElementException e ) {
      // no timestamp, ok since must be backwards compatible
      return null;
    }
  }
  private Long parseLong(String token) {
    try {
      return Long.valueOf(token);
    }
    catch (NumberFormatException e) {
      String s = "Attempt to set to non-numeric value: " + token;
      throw new HydraConfigException( s );
    }
  }
  private String parseJavaIdentifier( ListIterator i ) {
    // @todo lises check that this is a proper java identifier
    try {
      String token = (String) i.next();
      return token;
    } catch( NoSuchElementException e ) {
      throw new StatConfigException( "Missing java identifier" );
    }
  }
  private void parseEquals( ListIterator i ) {
    try {
      String token = (String) i.next();
      if ( ! token.equals( TrimSpecTokens.EQUALS ) ) {
        throw new StatConfigException( "Missing equals (=) operator, unexpected token: " + token );
      }
    } catch( NoSuchElementException e ) {
      throw new StatConfigException( "Missing equals (=) operator" );
    }
  }
  private String reconstruct( Vector stmt ) {
    return reconstructMostly( stmt ) + TrimSpecTokens.SEMI;
  }
  private String reconstructMostly( Vector stmt ) {
    StringBuffer buf = new StringBuffer(100);
    for ( ListIterator i = stmt.listIterator(); i.hasNext(); ) {
      buf.append( TrimSpecTokens.SPACE );
      buf.append( (String) i.next() );
    }
    return buf.toString();
  }
  private String getText( String fn ) throws FileNotFoundException {
    try {
      return FileUtil.getText( fn );
    } catch( FileNotFoundException e ) {
      throw e;
    } catch( IOException e ) {
      throw new StatConfigException( "Error reading trim specification file " + fn, e );
    }
  }
  public static void main( String args[] ) {
    Log.createLogWriter( "parser", "info" );
    String fn = args[0];
    StatConfig statconfig = new StatConfig( System.getProperty( "user.dir" ) );
    try {
      TrimSpecParser.parseFile( fn, statconfig );
      System.out.println( statconfig );
    } catch( FileNotFoundException e ) {
      System.out.println( "Trim specification file " + fn + " not found." );
      e.printStackTrace();
      System.exit(1);
    } catch ( StatConfigException e ) {
      System.out.println( "Error in trim specification file: " + fn );
      e.printStackTrace();
      System.exit(1);
    }
  }
  private static LogWriter log() {
    return Log.getLogWriter();
  }
}
