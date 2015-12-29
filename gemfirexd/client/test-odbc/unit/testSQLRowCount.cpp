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

#include "GemFireXDHelper.h"
using namespace std;

//*-------------------------------------------------------------------------

#define TESTNAME "SQLRowCount"
#define TABLE "ROWCOUNT"

#define MAX_NAME_LEN 50

//*-------------------------------------------------------------------------


BEGIN_TEST(testSQLRowCount)
{
  /* ------------------------------------------------------------------------- */
    CHAR                   tabname[MAX_NAME_LEN+1];
    CHAR                   create[MAX_NAME_LEN+1];
    CHAR                   update[MAX_NAME_LEN+1];
    CHAR                   deletestr[MAX_NAME_LEN+1];
    CHAR                   insert[MAX_NAME_LEN+1];
    CHAR             drop[MAX_NAME_LEN+1];
    SQLLEN      pcrow;
  /* ---------------------------------------------------------------------har- */

    //init sql handles (stmt, dbc, env)
    INIT_SQLHANDLES

  /* ------------------------------------------------------------------------- */

    /* --- Create Table --------------------------------------------- */
    strcpy(tabname, TABLE);
    strcpy(create,"CREATE TABLE ");
    strcat(create,tabname);
    strcat(create," (ID INTEGER, NAME VARCHAR(80), AGE SMALLINT)");
    printf("\tCreate Stmt = '%s'\r\n",create);

    retcode = SQLExecDirect(hstmt, (SQLCHAR*)create, SQL_NTS);
    DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,"SQLExecDirect");

    /* --- Insert Table --------------------------------------------- */
    /* 1. */
    strcpy(tabname,TABLE);
    strcpy(insert,"INSERT INTO ");
    strcat(insert,tabname);
    strcat(insert," VALUES (10, 'TestName', 40)");
    printf("\tInsert Stmt= '%s'\r\n",insert);

    retcode = SQLExecDirect(hstmt, (SQLCHAR*)insert,  SQL_NTS);
    DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,"SQLExecDirect");

    retcode = SQLExecDirect(hstmt, (SQLCHAR*)insert,  SQL_NTS);
    DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,"SQLExecDirect");

    retcode = SQLExecDirect(hstmt, (SQLCHAR*)insert,  SQL_NTS);
    DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,"SQLExecDirect");

    /* --- SQLRowCount ---------------------------------------------- */

    retcode = SQLRowCount(hstmt, &pcrow);
    DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,"SQLRowCount");

    printf("\tSQLRowCount= '%d'\r\n",pcrow);

    /* --- Update Table --------------------------------------------- */
    /* 1. */
    strcpy(tabname,TABLE);
    strcpy(update,"UPDATE ");
    strcat(update,tabname);
    strcat(update," SET NAME='xyz' ");
    printf("\tUpdate Stmt= '%s'\r\n",update);

    retcode = SQLExecDirect(hstmt, (SQLCHAR*)update, SQL_NTS);
    DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,"SQLExecDirect");

    /* --- SQLRowCount ---------------------------------------------- */

    retcode = SQLRowCount(hstmt, &pcrow);
    DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,"SQLRowCount");

    printf("\tSQLRowCount= '%d'\r\n",pcrow);

    /* --- Delete Table --------------------------------------------- */
    /* 1. */
    strcpy(tabname,TABLE);
    strcpy(deletestr,"DELETE FROM ");
    strcat(deletestr,tabname);
    strcat(deletestr," WHERE NAME='xyz'");
    printf("\tDelete Stmt= '%s'\r\n",deletestr);

    retcode = SQLExecDirect(hstmt, (SQLCHAR*)deletestr, SQL_NTS);
    DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,"SQLExecDirect");

    /* --- SQLRowCount ---------------------------------------------- */

    retcode = SQLRowCount(hstmt, &pcrow);
    DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,"SQLRowCount");

    printf("\tSQLRowCount= '%d'\r\n",pcrow);

    /* --- Drop Table ----------------------------------------------- */
    strcpy(tabname, TABLE);
    strcpy(drop,"DROP TABLE ");
    strcat(drop,tabname);
    printf("\tDrop Stmt= '%s'\r\n",drop);

    retcode = SQLExecDirect(hstmt, (SQLCHAR*)drop, SQL_NTS);
    DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,"SQLExecDirect");
  /* ---------------------------------------------------------------------har- */
  //free sql handles (stmt, dbc, env)
  FREE_SQLHANDLES
}
END_TEST(testSQLRowCount)
