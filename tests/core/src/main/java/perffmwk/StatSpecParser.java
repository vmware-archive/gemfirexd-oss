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
import com.gemstone.gemfire.internal.StatArchiveReader;

import hydra.*;

import java.io.*;
import java.text.*;
import java.util.*;

/** 
 *
 * Parses statistics specification files.
 *
 */

// @todo lises guard against cycles in include files

public class StatSpecParser {

  public static RuntimeStatSpec parseRuntimeStatSpec( String spec ) {
    ConfigLexer lex = new ConfigLexer();
    Vector stmt = ConfigLexer.tokenize( spec );
    StatSpecParser parser = new StatSpecParser();
    RuntimeStatSpec statspec = parser.parseRuntimeStatSpec( stmt );
    return statspec;
  }
  public static void parseFile( String fn, StatConfig statconfig ) throws FileNotFoundException {
    parseFile( fn, statconfig, false );
  }

  public static void parseFile( String fn, StatConfig statconfig, boolean verbose ) throws FileNotFoundException {

    StatSpecParser parser = new StatSpecParser();

    // read file contents
    if (verbose) log().info( "\n/////  BEGIN " + fn );
    String text = parser.getText( fn );
    if (verbose) log().info( "\n" + text );

    // get tokens
    ConfigLexer lex = new ConfigLexer();
    Vector tokens = ConfigLexer.tokenize( text );
    if ( tokens == null ) {
      log().warning( "Statistics specification file " + fn + " is empty" );
      return;
    }
    //for ( ListIterator i = tokens.listIterator(); i.hasNext(); )
    //  log().info( i.next() );
    //log().info( tokens );

    // parse
    parser.parse( statconfig, tokens );

    if (verbose) log().info( "\n/////  END   " + fn );
  }
  //// RULE: config = (stmt StatSpecTokens.SEMI)*
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
          try {
	    //if ( log().finestEnabled() )
	    //  log().finest( "SENT: " + token );
            token = expandSystemProperties( statconfig, token );
	    //if ( log().finestEnabled() )
	    //  log().finest( "GOT:  " + token );
          } catch( StatConfigException e ) {
	    stmt.add( token );
	    stmt.add( "..." );
            e.printStackTrace();
            throw new StatConfigException( "Malformed statement:" + reconstruct( stmt ), e );
          }
	  stmt.add( token );
	}
      }
    }
    if ( stmt.size() > 0 ) {
      throw new StatConfigException( "Missing semicolon delimiter (;) at end of statement:" + reconstructMostly( stmt ) );
    }
  }
  //// RULE: stmt = include | includeifpresent | statspec | expr
  private void parseStmt( StatConfig statconfig, Vector stmt ) {
    String token = (String) stmt.elementAt(0);
    if ( token.equalsIgnoreCase( StatSpecTokens.INCLUDE ) )
      parseInclude( statconfig, stmt );
    else if ( token.equalsIgnoreCase( StatSpecTokens.INCLUDEIFPRESENT ) )
      parseIncludeIfPresent( statconfig, stmt );
    else if ( token.equalsIgnoreCase( StatSpecTokens.STATSPEC ) )
      parseStatSpec( statconfig, stmt );
    else if ( token.equalsIgnoreCase( StatSpecTokens.EXPR ) )
      parseExpr( statconfig, stmt );
    else
      throw new StatConfigException( "Unknown statement type: " + token );
  }
  private void parseInclude( StatConfig statconfig, Vector stmt ) {
    ListIterator i = stmt.listIterator();
    i.next(); // move past include keyword
    String fn = parseIdentifier( i );
    if (i.hasNext()) {
       throw new StatConfigException("Missing ; at end of " + StatSpecTokens.INCLUDE + " statement: " + stmt);
    }
    try {
      try {
        // assume this is a command line use case
        fn = EnvHelper.expandEnvVars( fn );
      } catch( HydraConfigException e ) {
        // ok, see if this is a test runtime use case
        fn = EnvHelper.expandEnvVars( fn, TestConfig.getInstance().getMasterDescription().getVmDescription().getHostDescription() );
      }
      StatSpecParser.parseFile( fn, statconfig );
    } catch( FileNotFoundException e ) {
      return;
    } catch( StatConfigException e ) {
      throw new StatConfigException( "Error in statistics specification file: " + fn, e );
    }
  }
  private void parseIncludeIfPresent( StatConfig statconfig, Vector stmt ) {
    ListIterator i = stmt.listIterator();
    i.next(); // move past includeifpresent keyword
    String fn = parseIdentifier( i );
    if (i.hasNext()) {
       throw new StatConfigException("Missing ; at end of " + StatSpecTokens.INCLUDEIFPRESENT + " statement: " + stmt);
    }
    try {
      try {
        // assume this is a command line use case
        fn = EnvHelper.expandEnvVars( fn );
      } catch( HydraConfigException e ) {
        // ok, see if this is a test runtime use case
        fn = EnvHelper.expandEnvVars( fn, TestConfig.getInstance().getMasterDescription().getVmDescription().getHostDescription() );
      }
      StatSpecParser.parseFile( fn, statconfig );
    } catch( FileNotFoundException e ) {
      // ignore
    } catch( StatConfigException e ) {
      throw new StatConfigException( "Error in statistics specification file: " + fn, e );
    }
  }
  private void parseStatSpec( StatConfig statconfig, Vector stmt ) {
    StatSpec statspec = parseStatSpec( stmt );
    statspec.setStatConfig( statconfig );
    statconfig.addStatSpec( statspec );
  }
  private void parseExpr(StatConfig statconfig, Vector stmt) {
    Expr expr = parseExprParts(statconfig, stmt);
    expr.setStatConfig(statconfig);
    statconfig.addStatSpec(expr);
  }
  private RuntimeStatSpec parseRuntimeStatSpec( Vector stmt ) {
    ListIterator i = stmt.listIterator();
    RuntimeStatSpec statspec = new RuntimeStatSpec();
    parseStatSpecId( i, statspec );
    parseStatSpecParams( i, statspec );
    return statspec;
  }
  private StatSpec parseStatSpec( Vector stmt ) {
    ListIterator i = stmt.listIterator();
    i.next(); // move past spec keyword
    String name = parseJavaIdentifier( i );
    StatSpec statspec = new StatSpec( name );
    parseStatSpecId( i, statspec );
    parseStatSpecParams( i, statspec );
    return statspec;
  }
  /** Form is expr = numerator / denominator ops = min, max; */
  private Expr parseExprParts(StatConfig statconfig, Vector stmt) {
    ListIterator i = stmt.listIterator();
    i.next(); // move past expr keyword
    String name = parseJavaIdentifier(i);
    Expr expr = new Expr(name);
    parseEquals(i);
    String numerator = parseJavaIdentifier(i);
    StatSpec numeratorSpec = statconfig.getStatSpec(numerator);
    if (numeratorSpec == null) {
      String s = "Expr " + name + " cannot find statspec " + numerator;
      throw new HydraConfigException(s);
    }
    expr.setNumerator(numeratorSpec);
    parseDiv(i);
    String denominator = parseJavaIdentifier(i);
    StatSpec denominatorSpec = statconfig.getStatSpec(denominator);
    if (denominatorSpec == null) {
      String s = "Expr " + name + " cannot find statspec " + denominator;
      throw new HydraConfigException(s);
    }
    expr.setDenominator(denominatorSpec);
    if (!i.hasNext()) {
      String s = "Expr " + name + " is missing " + StatSpecTokens.OP_TYPES;
      throw new HydraConfigException(s);
    }
    String token = (String)i.next();
    if (token.equalsIgnoreCase(StatSpecTokens.OP_TYPES)) {
      parseOpsParam(i, expr);
    } else {
      throw new StatConfigException("Unexpected token: " + token);
    }
    return expr;
  }
  private void parseStatSpecId( ListIterator i, StatSpec statspec ) {
    StatSpecId statspecid = new StatSpecId();
    statspecid.setSystemName( parseStatSpecIdComponent( i, statspec, "system name", true ) );
    statspecid.setTypeName( parseStatSpecIdComponent( i, statspec, "type name", true ) );
    statspecid.setInstanceName( parseStatSpecIdComponent( i, statspec, "instance name", true ) );
    statspecid.setStatName( parseStatSpecIdComponent( i, statspec, "stat name", true ) );
    statspec.setId( statspecid );
  }
  private String parseStatSpecIdComponent( ListIterator i, StatSpec statspec, String component, boolean required ) {
    String s = null;
    try {
      s = (String) i.next();
      if ( s.equalsIgnoreCase( StatSpecTokens.FILTER_TYPE )
        || s.equalsIgnoreCase( StatSpecTokens.COMBINE_TYPE )
        || s.equalsIgnoreCase( StatSpecTokens.OP_TYPES )
	|| s.equalsIgnoreCase( StatSpecTokens.TRIM_SPEC_NAME) )
      {
	if ( required ) {
          malformedStatSpecId( component );
        } else {
	  i.previous();
	  return null;
        }
      }
    } catch( NoSuchElementException e ) {
      if ( required )
        malformedStatSpecId( component );
    }
    return s;
  }
  private void malformedStatSpecId( String component ) {
    throw new StatConfigException( "Missing " + component + ", stat spec id must be specified as <system_name> <type_name> <instance_name> <stat_name>" );
  }
  private void parseStatSpecParams( ListIterator i, StatSpec statspec ) {
    while ( i.hasNext() ) {
      String token = (String) i.next();
      if ( token.equalsIgnoreCase( StatSpecTokens.FILTER_TYPE ) )
        parseFilterParam( i, statspec );
      else if ( token.equalsIgnoreCase( StatSpecTokens.COMBINE_TYPE ) )
        parseCombineParam( i, statspec );
      else if ( token.equalsIgnoreCase( StatSpecTokens.OP_TYPES ) )
        parseOpsParam( i, statspec );
      else if ( token.equalsIgnoreCase( StatSpecTokens.TRIM_SPEC_NAME ) )
        parseTrimSpecNameParam( i, statspec );
      else
        throw new StatConfigException( "Unexpected token: " + token );
    }
  }
  private void parseFilterParam( ListIterator i, StatSpec statspec ) {
    parseEquals( i );
    parseFilter( i, statspec );
  }
  private void parseFilter( ListIterator i, StatSpec statspec ) {
    try {
      String filter = (String) i.next();
      if ( filter.equalsIgnoreCase( StatSpecTokens.FILTER_PERSEC ) )
        statspec.setFilter( StatArchiveReader.StatValue.FILTER_PERSEC );
      else if ( filter.equalsIgnoreCase( StatSpecTokens.FILTER_NONE ) )
        statspec.setFilter( StatArchiveReader.StatValue.FILTER_NONE );
      else
        throw new StatConfigException( "Illegal " + StatSpecTokens.FILTER_TYPE + ": " + filter );
    } catch( NoSuchElementException e ) {
      throw new StatConfigException( "Missing " + StatSpecTokens.FILTER_TYPE );
    }
  }
  private void parseCombineParam( ListIterator i, StatSpec statspec ) {
    parseEquals( i );
    parseCombine( i, statspec );
  }
  private void parseCombine( ListIterator i, StatSpec statspec ) {
    try {
      String combine = (String) i.next();
      if ( combine.equalsIgnoreCase( StatSpecTokens.RAW ) )
        statspec.setCombineType( StatArchiveReader.StatSpec.NONE );
      else if ( combine.equalsIgnoreCase( StatSpecTokens.COMBINE ) )
        statspec.setCombineType( StatArchiveReader.StatSpec.FILE );
      else if ( combine.equalsIgnoreCase( StatSpecTokens.COMBINE_ACROSS_ARCHIVES ) )
        statspec.setCombineType( StatArchiveReader.StatSpec.GLOBAL );
      else
        throw new StatConfigException( "Illegal " + StatSpecTokens.COMBINE_TYPE + ": " + combine );
    } catch( NoSuchElementException e ) {
      throw new StatConfigException( "Missing " + StatSpecTokens.COMBINE_TYPE );
    }
  }
  private void parseOpsParam( ListIterator i, StatSpec statspec ) {
    parseEquals( i );
    parseOps( i, statspec );
  }
  private void parseOps( ListIterator i, StatSpec statspec ) {
    Vector ops = parseUnterminatedCommaIdentifierList( i );
    if ( ops.size() == 0 )
      throw new StatConfigException( "Missing " + StatSpecTokens.OP_TYPES );
    for ( Iterator it = ops.iterator(); it.hasNext(); ) {
      String op = (String) it.next();
      if ( op.equalsIgnoreCase( StatSpecTokens.MIN ) ) {
        statspec.setMin( true );
      } else if ( op.equalsIgnoreCase( StatSpecTokens.MAX ) ) {
        statspec.setMax( true );
      } else if ( op.equalsIgnoreCase( StatSpecTokens.MAXMINUSMIN ) ) {
        statspec.setMaxMinusMin( true );
      } else if ( op.equalsIgnoreCase( StatSpecTokens.MEAN ) ) {
        statspec.setMean( true );
      } else if ( op.equalsIgnoreCase( StatSpecTokens.STDDEV ) ) {
        statspec.setStddev( true );
      } else if ( op.equalsIgnoreCase( StatSpecTokens.MIN_E ) ) {
        statspec.setMin( true );
        statspec.setMinExpr( true );
      } else if ( op.equalsIgnoreCase( StatSpecTokens.MAX_E ) ) {
        statspec.setMax( true );
        statspec.setMaxExpr( true );
      } else if ( op.equalsIgnoreCase( StatSpecTokens.MAXMINUSMIN_E ) ) {
        statspec.setMaxMinusMin( true );
        statspec.setMaxMinusMinExpr( true );
      } else if ( op.equalsIgnoreCase( StatSpecTokens.MEAN_E ) ) {
        statspec.setMean( true );
        statspec.setMeanExpr( true );
      } else if ( op.equalsIgnoreCase( StatSpecTokens.STDDEV_E ) ) {
        statspec.setStddev( true );
        statspec.setStddevExpr( true );
      } else if ( op.equalsIgnoreCase( StatSpecTokens.MIN_C ) ) {
        statspec.setMin( true );
        statspec.setMinCompare( true );
      } else if ( op.equalsIgnoreCase( StatSpecTokens.MAX_C ) ) {
        statspec.setMax( true );
        statspec.setMaxCompare( true );
      } else if ( op.equalsIgnoreCase( StatSpecTokens.MAXMINUSMIN_C ) ) {
        statspec.setMaxMinusMin( true );
        statspec.setMaxMinusMinCompare( true );
      } else if ( op.equalsIgnoreCase( StatSpecTokens.MEAN_C ) ) {
        statspec.setMean( true );
        statspec.setMeanCompare( true );
      } else if ( op.equalsIgnoreCase( StatSpecTokens.STDDEV_C ) ) {
        statspec.setStddev( true );
        statspec.setStddevCompare( true );
      } else {
        throw new StatConfigException( "Illegal value for " + StatSpecTokens.OP_TYPES );
      }
    }
  }
  private void parseTrimSpecNameParam( ListIterator i, StatSpec statspec ) {
    parseEquals( i );
    String trimspecName = parseJavaIdentifier( i );
    statspec.setTrimSpecName( trimspecName );
  }
  private Vector parseUnterminatedCommaIdentifierList( ListIterator i ) {
    // identifier (COMMA identifier)*
    Vector list = new Vector();

    // get the first one: identifier
    String identifier = parseIdentifier( i );
    if ( identifier.equals( StatSpecTokens.COMMA ) )
      throw new StatConfigException( "Found comma instead of identifier" );
    else // assume real value
      list.add( identifier );

    // get the rest: (COMMA identifier)*
    while ( i.hasNext() ) {
      identifier = parseIdentifier( i );
      if ( identifier.equals( StatSpecTokens.COMMA ) ) { // look for real value
        identifier = parseIdentifier( i );
	if ( identifier.equals( StatSpecTokens.COMMA ) ) {
	  throw new StatConfigException( "Extra comma" );
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
      if ( ! token.equals( StatSpecTokens.EQUALS ) ) {
        throw new StatConfigException( "Missing equals (=) operator, unexpected token: " + token );
      }
    } catch( NoSuchElementException e ) {
      throw new StatConfigException( "Missing equals (=) operator" );
    }
  }
  private void parseDiv(ListIterator i) {
    try {
      String token = (String)i.next();
      if (!token.equals(StatSpecTokens.DIV)) {
        throw new StatConfigException("Missing division (/) operator, unexpected token: " + token);
      }
    } catch (NoSuchElementException e) {
      throw new StatConfigException("Missing division (/) operator");
    }
  }
  private String parseIdentifier( ListIterator i ) {
    // @todo lises check that this is a proper identifier (could start with $)
    try {
      String token = (String) i.next();
      return token;
    } catch( NoSuchElementException e ) {
      throw new StatConfigException( "Missing identifier" );
    }
  }
  private String expandSystemProperties( StatConfig statconfig, String token ) {
    StringBuffer buf = new StringBuffer();
    StringCharacterIterator i = new StringCharacterIterator( token );
    while ( i.current() != CharacterIterator.DONE ) {
      char c = i.current();
      switch( c ) {
        case '$':
          char cc = i.next();
          switch( cc ) {
            case '{':
	      gobbleProperty( statconfig, i, buf );
              break;
            default:
	      //if ( log().finestEnabled() )
	      //  log().finest( String.valueOf( c ) );
	      buf.append( c );
	      //if ( log().finestEnabled() )
	      //  log().finest( String.valueOf( cc ) );
	      buf.append( cc );
              break;
          }
          break;
        default:
	  if ( c != CharacterIterator.DONE ) {
	    //if ( log().finestEnabled() )
	    //  log().finest( String.valueOf( c ) );
            buf.append( c );
          }
          break;
      }
      i.next();
    }
    return buf.toString();
  }
  private void gobbleProperty( StatConfig statconfig, StringCharacterIterator i, StringBuffer buf ) {
    // invoked after seeing "${"
    StringBuffer propKey = new StringBuffer();
    while ( i.current() != CharacterIterator.DONE ) {
      char c = i.next();
      switch( c ) {
        case '}':
          String propVal = System.getProperty( propKey.toString() );
          if ( propVal == null ) {
	    String testDir = statconfig.getTestDir();
	    String testName = statconfig.getTestName();
	    if ( testDir == null || testName == null ) {
              throw new StatConfigException( "No system property set for " + propKey );
	    }
            testName = testName.substring( testName.lastIndexOf( "/" ), testName.lastIndexOf( "." ) );
            String propFile = testDir + testName + ".prop";
            log().info( "Reading system properties from " + propFile );
            try {
              Properties props = FileUtil.getProperties( propFile );
              propVal = props.getProperty( propKey.toString() );
              if ( propVal == null ) {
                throw new StatConfigException( "No system property set for " + propKey );
              }
            } catch( IOException e ) {
              throw new StatConfigException( "No system property set for " + propKey );
            }
          }
	  //log().finest( propVal );
          buf.append( propVal );
	  statconfig.addSystemProperty( propKey.toString(), propVal );
          return;
        default:
	  if ( c != CharacterIterator.DONE ) {
	    //log().finest( "PROPKEY: " + c );
            propKey.append( c );
	  }
          break;
      }
    }
    throw new StatConfigException( "Incomplete system property: " + propKey );
  }
  private String reconstruct( Vector stmt ) {
    return reconstructMostly( stmt ) + StatSpecTokens.SEMI;
  }
  private String reconstructMostly( Vector stmt ) {
    StringBuffer buf = new StringBuffer(100);
    for ( ListIterator i = stmt.listIterator(); i.hasNext(); ) {
      buf.append( StatSpecTokens.SPACE );
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
      throw new StatConfigException( "Error reading statistics specification file " + fn, e );
    }
  }

  public static void main( String args[] ) {
    Log.createLogWriter( "parser", "info" );
    String fn = args[0];
    StatConfig statconfig = new StatConfig( System.getProperty( "user.dir" ) );
    try {
      StatSpecParser.parseFile( fn, statconfig );
      System.out.println( statconfig );
    } catch( FileNotFoundException e ) {
      System.out.println( "Statistics specification file " + fn + " not found." );
      e.printStackTrace();
      System.exit(1);
    } catch ( StatConfigException e ) {
      System.out.println( "Error in statistics specification file: " + fn );
      e.printStackTrace();
      System.exit(1);
    }
  }
  private static LogWriter log() {
    return Log.getLogWriter();
  }
}
