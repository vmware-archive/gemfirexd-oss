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

import java.text.*;
import java.util.*;

/** 
*
* Lexes hydra config files.
*
*/

public class ConfigLexer {

  private static final char PLUS     = '+';
  private static final char EQUALS   = '=';
  private static final char COMMA    = ',';
  private static final char SEMI     = ';';

  private static final char ESC      = '\\';
  private static final char POUND    = '#';
  private static final char SLASH    = '/';
  private static final char STAR     = '*';
  private static final char QUOTE    = '"';

  private static final char SPACE    = ' ';
  private static final char TAB      = '\t';
  private static final char NEWLINE  = '\n';
  private static final char RETURN   = '\r';
  private static final char FORMFEED = '\f';

  private static final char EOF = CharacterIterator.DONE;

  public static Vector tokenize( String text ) {
    ConfigLexer lex = new ConfigLexer();
    Vector tokens = lex.getTokens( text );
    return tokens;
  }
  private Vector getTokens( String text ) {
    if ( text == null ) return null;
    Vector tokens = new Vector();
    StringCharacterIterator sci = new StringCharacterIterator( text );
    while ( sci.current() != EOF ) {
      String token = nextToken( sci );
      if ( token != null ) {
        tokens.add( token );
      }
      //Log.getLogWriter().finest( "index:" + sci.getIndex() + " TOKEN:" + token );
    }
    return tokens;
  }
  private StringBuffer token = new StringBuffer(100);
  private String nextToken( StringCharacterIterator sci ) {
    token.setLength( 0 );
    switch( sci.current() ) {
      case SPACE   :
      case FORMFEED:
      case NEWLINE :
      case RETURN  :
      case TAB     : gobbleWhiteSpace( sci );           break;
      case PLUS    :
      case EQUALS  :
      case COMMA   :
      case SEMI    : matchDelimiter( sci, token );      break;
      case QUOTE   : matchString( sci, token );         break;
      case POUND   : matchComment( sci );               break;
      case SLASH   : matchCommentOrValue( sci, token ); break;
      default      : matchValue( sci, token );          break;
    }
    return ( token.length() > 0 ) ? token.toString() : null;
  }
  // this is called when a token starts with whitespace character
  private void gobbleWhiteSpace( StringCharacterIterator sci ) {
    for ( char c = sci.current(); c != EOF; c = sci.next() ) {
      switch( c ) {
        case SPACE   :
        case FORMFEED:
        case NEWLINE :
        case RETURN  :
        case TAB     : break;
        default      : return;
      }
    }
  }
  // this is called when a token starts with a delimiter character
  private void matchDelimiter( StringCharacterIterator sci, StringBuffer token ) {
    char c = sci.current();
    switch( c ) {
      case PLUS : char cnext= sci.next();
                  switch( cnext) {
                    case EQUALS : token.append( c );
                                  token.append( cnext );
                                  sci.next();
                                  break;
                    default     : sci.previous();
                                  matchValue( sci, token );
                  }
                  break;
      default   : token.append( c );
                  sci.next();
    }
  }
  // this is called when a token starts with QUOTE
  private void matchString( StringCharacterIterator sci, StringBuffer token ) {
    for ( char c = sci.next(); c != EOF; c = sci.next() ) {
      switch( c ) {
        case ESC   : char cnext = sci.next();
                     if ( cnext != EOF )
                       token.append( cnext );
                     break;
        case QUOTE : sci.next();
                     return;
        default    : token.append( c );
      }
    }
    throw new HydraConfigException( "Premature end of file in string" );
  }
  // this is called when a token starts with POUND
  private void matchComment( StringCharacterIterator sci) {
    gobbleSingleLineComment(sci);
  }
  // this is called when a token starts with SLASH
  private void matchCommentOrValue( StringCharacterIterator sci, StringBuffer token ) {
    char cnext = sci.next();
    if ( cnext == EOF ) {
      throw new HydraConfigException( "Premature end of file in comment or value" );
    } else if ( cnext == SLASH ) {
      sci.next();
      gobbleSingleLineComment( sci );
    } else if ( cnext == STAR ) {
      sci.next();
      gobbleMultiLineComment( sci );
    } else { // value
      sci.previous(); // reset index
      matchValue( sci, token );
    }
  }
  // this is called when a token starts with SLASH-SLASH OR POUND
  private void gobbleSingleLineComment( StringCharacterIterator sci ) {
    for ( char c = sci.current(); c != EOF; c = sci.next() ) {
      switch( c ) {
        case NEWLINE: return;
      }
    }
  }
  // this is called when a token starts with SLASH-STAR
  private void gobbleMultiLineComment( StringCharacterIterator sci ) {
    for ( char c = sci.current(); c != EOF; c = sci.next() ) {
      switch( c ) {
        case STAR: // possible end of comment, need to look ahead
                   char cnext = sci.next();
                   if ( cnext == EOF ) {
                     throw new HydraConfigException( "Premature end of file in multi-line comment" );
                   } else if ( cnext == SLASH ) {
                     sci.next();
                     return;
                   } // else just more comment
                   break;
      }
    }
    throw new HydraConfigException( "Premature end of file in multi-line comment" );
  }
  // this is called when a token starts with anything else
  private void matchValue( StringCharacterIterator sci, StringBuffer token ) {
    for ( char c = sci.current(); c != EOF; c = sci.next() ) {
      switch( c ) {
        case SPACE   :
        case FORMFEED:
        case NEWLINE :
        case RETURN  :
        case TAB     : sci.next();
                       return;
        case EQUALS  :
        case COMMA   :
        case SEMI    : return;

        default      : token.append( c );
      }
    }
  }
}
