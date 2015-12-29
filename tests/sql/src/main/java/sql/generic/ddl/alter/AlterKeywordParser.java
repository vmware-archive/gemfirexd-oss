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
package sql.generic.ddl.alter;


public enum AlterKeywordParser {
  COLUMN(" column ")  ,
  COLUMN_NAME("column-name")  ,
  PRIMARYKEY(" primary key" ) ,
  PRIMARY_KEY("primary-key" ) ,
  FOREIGNKEY(" foreign key" ) ,
  UNIQ(" unique" ) ,
  CHECK(" check" ) ,
  CONSTRAINT(" constraint" ),
  EVICTION(" eviction maxsize " ) ,
  GATEWAYSENDER(" gatewaysender " ),
  LISTENER(" asynceventlistener" ),
  UNSUPPORTED("unsupported");

  String keyword;

  AlterKeywordParser(String keyword  ) {
  this.keyword = keyword;
    }

  public String getKeyword (){
    return keyword.toUpperCase();
  }

  public static AlterKeywordParser getKeyWordOperation(String ddl) {
    if ( ddl.contains (PRIMARYKEY.getKeyword()) || ddl.contains (PRIMARY_KEY.getKeyword())) {
      return PRIMARYKEY;
    }else if ( ddl.contains (FOREIGNKEY.getKeyword())) {
      return FOREIGNKEY;
    }else if ( ddl.contains (UNIQ.getKeyword())) {
      return UNIQ;
    }else if ( ddl.contains (CHECK.getKeyword())) {
      return CHECK;
    }else if ( ddl.contains (CONSTRAINT.getKeyword())) {
      return CONSTRAINT;
    }else  if ( ddl.contains (COLUMN.getKeyword()) || ddl.contains(COLUMN_NAME.getKeyword()) ) {
        return COLUMN;
    }else if ( ddl.contains (EVICTION.getKeyword())) {
      return EVICTION;
    }else if ( ddl.contains (GATEWAYSENDER.getKeyword())) {
      return GATEWAYSENDER;
    } else if ( ddl.contains (LISTENER.getKeyword())) {
      return LISTENER;
  } else  return UNSUPPORTED;
}
}
