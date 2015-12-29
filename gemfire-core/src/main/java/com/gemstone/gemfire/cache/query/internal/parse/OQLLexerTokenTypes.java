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
// $ANTLR 2.7.4: "oql.g" -> "OQLParser.java"$

package com.gemstone.gemfire.cache.query.internal.parse;
import java.util.*;
import com.gemstone.gemfire.cache.query.internal.types.*;

public interface OQLLexerTokenTypes {
	int EOF = 1;
	int NULL_TREE_LOOKAHEAD = 3;
	int TOK_RPAREN = 4;
	int TOK_LPAREN = 5;
	int TOK_COMMA = 6;
	int TOK_SEMIC = 7;
	int TOK_DOTDOT = 8;
	int TOK_COLON = 9;
	int TOK_DOT = 10;
	int TOK_INDIRECT = 11;
	int TOK_CONCAT = 12;
	int TOK_EQ = 13;
	int TOK_PLUS = 14;
	int TOK_MINUS = 15;
	int TOK_SLASH = 16;
	int TOK_STAR = 17;
	int TOK_LE = 18;
	int TOK_GE = 19;
	int TOK_NE = 20;
	int TOK_NE_ALT = 21;
	int TOK_LT = 22;
	int TOK_GT = 23;
	int TOK_LBRACK = 24;
	int TOK_RBRACK = 25;
	int TOK_DOLLAR = 26;
	int LETTER = 27;
	int DIGIT = 28;
	int ALL_UNICODE = 29;
	int NameFirstCharacter = 30;
	int NameCharacter = 31;
	int RegionNameCharacter = 32;
	int QuotedIdentifier = 33;
	int Identifier = 34;
	int RegionPath = 35;
	int NUM_INT = 36;
	int EXPONENT = 37;
	int FLOAT_SUFFIX = 38;
	int HEX_DIGIT = 39;
	int QUOTE = 40;
	int StringLiteral = 41;
	int WS = 42;
	int SL_COMMENT = 43;
	int ML_COMMENT = 44;
	int QUERY_PROGRAM = 45;
	int QUALIFIED_NAME = 46;
	int QUERY_PARAM = 47;
	int ITERATOR_DEF = 48;
	int PROJECTION_ATTRS = 49;
	int PROJECTION = 50;
	int TYPECAST = 51;
	int COMBO = 52;
	int METHOD_INV = 53;
	int POSTFIX = 54;
	int OBJ_CONSTRUCTOR = 55;
	int IMPORTS = 56;
	int SORT_CRITERION = 57;
	int LIMIT = 58;
	int LITERAL_trace = 59;
	int LITERAL_import = 60;
	int LITERAL_as = 61;
	int LITERAL_declare = 62;
	int LITERAL_define = 63;
	int LITERAL_query = 64;
	int LITERAL_undefine = 65;
	int LITERAL_select = 66;
	int LITERAL_distinct = 67;
	int LITERAL_all = 68;
	int LITERAL_from = 69;
	int LITERAL_in = 70;
	int LITERAL_type = 71;
	int LITERAL_where = 72;
	int LITERAL_limit = 73;
	int LITERAL_group = 74;
	int LITERAL_by = 75;
	int LITERAL_having = 76;
	int LITERAL_order = 77;
	int LITERAL_asc = 78;
	int LITERAL_desc = 79;
	int LITERAL_or = 80;
	int LITERAL_orelse = 81;
	int LITERAL_and = 82;
	int LITERAL_for = 83;
	int LITERAL_exists = 84;
	int LITERAL_andthen = 85;
	int LITERAL_any = 86;
	int LITERAL_some = 87;
	int LITERAL_like = 88;
	int LITERAL_union = 89;
	int LITERAL_except = 90;
	int LITERAL_mod = 91;
	int LITERAL_intersect = 92;
	int LITERAL_abs = 93;
	int LITERAL_not = 94;
	int LITERAL_listtoset = 95;
	int LITERAL_element = 96;
	int LITERAL_flatten = 97;
	int LITERAL_nvl = 98;
	int LITERAL_to_date = 99;
	int LITERAL_first = 100;
	int LITERAL_last = 101;
	int LITERAL_unique = 102;
	int LITERAL_sum = 103;
	int LITERAL_min = 104;
	int LITERAL_max = 105;
	int LITERAL_avg = 106;
	int LITERAL_count = 107;
	int LITERAL_is_undefined = 108;
	int LITERAL_is_defined = 109;
	int LITERAL_struct = 110;
	int LITERAL_array = 111;
	int LITERAL_set = 112;
	int LITERAL_bag = 113;
	int LITERAL_list = 114;
	int LITERAL_short = 115;
	int LITERAL_long = 116;
	int LITERAL_int = 117;
	int LITERAL_float = 118;
	int LITERAL_double = 119;
	int LITERAL_char = 120;
	int LITERAL_string = 121;
	int LITERAL_boolean = 122;
	int LITERAL_byte = 123;
	int LITERAL_octet = 124;
	int LITERAL_enum = 125;
	int LITERAL_date = 126;
	int LITERAL_time = 127;
	int LITERAL_interval = 128;
	int LITERAL_timestamp = 129;
	int LITERAL_collection = 130;
	int LITERAL_dictionary = 131;
	int LITERAL_map = 132;
	int LITERAL_nil = 133;
	int LITERAL_null = 134;
	int LITERAL_undefined = 135;
	int LITERAL_true = 136;
	int LITERAL_false = 137;
	int NUM_LONG = 138;
	int NUM_FLOAT = 139;
	int NUM_DOUBLE = 140;
}
