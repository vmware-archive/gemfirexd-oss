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

#define TESTNAME "SQLCancel"
#define TABLE "SQLCANCEL1"

#define ERROR_TEXT_LEN 511
#define MAX_NAME_LEN 50

class SQLCancelTask : public ACE_Task_Base
{
private:
  ACE_Barrier& m_barrier;
  SQLHSTMT m_hstmt;

public:
  inline SQLCancelTask(ACE_Barrier& barrier, SQLHSTMT hstmt) :
      m_barrier(barrier), m_hstmt(hstmt)
  {
  }

  int svc()
  {
    int status = 0;
    try
    {
      LOG("Cancellation thread started");
      // wait for all threads
      m_barrier.wait();

      //sleep some time for the query execution to proceed
      ACE_OS::sleep(5);

      LOGF("HSTMT =%d", m_hstmt);
      //cancel the statement
      SQLRETURN retcode = SQLCancel(m_hstmt);
      DIAGRECCHECK(SQL_HANDLE_STMT, m_hstmt, 1, SQL_SUCCESS, retcode, "SQLCancel");
    }
    catch (std::exception& ex) {
      LOGF("ERROR: failed with exception: %s",ex.what());
    } catch (...) {
      LOG("ERROR: failed with unknown exception...");
    }
    return status;
  }
};

//*-------------------------------------------------------------------------

BEGIN_TEST(testSQLCancelMT)
{
  /* ------------------------------------------------------------------------- */
      SQLCHAR sqlstate[10];
      SQLINTEGER esq_sql_code;
      SQLCHAR error_txt[ERROR_TEXT_LEN + 1];
      SQLSMALLINT len_error_txt = ERROR_TEXT_LEN;
      SQLSMALLINT used_error_txt;
      char buffer[1024];

      /* ---------------------------------------------------------------------har- */
      //initialize the sql handles
      INIT_SQLHANDLES

      /* ------------------------------------------------------------------------- */

      /* --- Create Table --------------------------------------------- */
      LOGF("Create Stmt = 'CREATE TABLE SQLCANCEL2(ID INTEGER, NAME VARCHAR(80), AGE SMALLINT)'");
      retcode = SQLExecDirect(hstmt, (SQLCHAR*)"CREATE TABLE SQLCANCEL2(ID INTEGER, NAME VARCHAR(80), AGE SMALLINT)", SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode, "SQLExecDirect");

      /* --- Insert values --------------------------------------------- */
      LOGF("Insert Stmt= 'INSERT INTO SQLCANCEL2 VALUES (?, ?, ?)'");
      retcode = SQLPrepare(hstmt, (SQLCHAR*)"INSERT INTO SQLCANCEL2 VALUES (?, ?, ?)",  SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode, "SQLPrepare");

      int id = 0;
      int age = 0;
      char name[100];
      retcode = SQLBindParameter(hstmt, 1, SQL_PARAM_INPUT, SQL_C_LONG,
               SQL_INTEGER, 0, 0, &id, 0, NULL);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
               "SQLBindParameter (SQL_INTEGER)");

      retcode = SQLBindParameter(hstmt, 2, SQL_PARAM_INPUT, SQL_C_CHAR,
                SQL_CHAR, MAX_NAME_LEN, 0, name, 0, NULL);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
                "SQLBindParameter (SQL_CHAR)");

      retcode = SQLBindParameter(hstmt, 3, SQL_PARAM_INPUT, SQL_C_LONG,
               SQL_INTEGER, 0, 0, &age, 0, NULL);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
               "SQLBindParameter (SQL_INTEGER)");
      for(int i = 1; i<=10; i++)
      {
    	  id = i;
    	  age = i * 5;
    	  sprintf(name, "User%d",i);
    	  retcode = SQLExecute(hstmt);
    	  DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode, "SQLExecute");
      }

      retcode = SQLFreeStmt(hstmt, SQL_RESET_PARAMS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode, "SQLFreeStmt");


      /* --- create DAPs for adding/removing observer --------------------------------------- */
      retcode = SQLExecDirect(hstmt, (SQLCHAR*)"DROP PROCEDURE IF EXISTS PROC_ADD_QUERY_OBSRVR", SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode, "SQLExecDirect");

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)"DROP PROCEDURE IF EXISTS PROC_REMOVE_QUERY_OBSRVR", SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode, "SQLExecDirect");

      LOG("\tCreate stored proedure Stmt= 'CREATE PROCEDURE PROC_ADD_QUERY_OBSRVR(sleepTime int) LANGUAGE JAVA "
    		  "PARAMETER STYLE JAVA EXTERNAL NAME 'tests.TestProcedures.addQueryObserver''");
      retcode = SQLExecDirect(hstmt, (SQLCHAR*)"CREATE PROCEDURE PROC_ADD_QUERY_OBSRVR(sleepTime int) LANGUAGE JAVA "
    		  "PARAMETER STYLE JAVA EXTERNAL NAME 'tests.TestProcedures.addQueryObserver'", SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode, "SQLExecDirect");

      LOG("\tCreate stored proedure Stmt= 'CREATE PROCEDURE PROC_REMOVE_QUERY_OBSRVR() LANGUAGE JAVA "
          		  "PARAMETER STYLE JAVA EXTERNAL NAME 'tests.TestProcedures.removeQueryObserver''");
      retcode = SQLExecDirect(hstmt, (SQLCHAR*)"CREATE PROCEDURE PROC_REMOVE_QUERY_OBSRVR() LANGUAGE JAVA "
          		  "PARAMETER STYLE JAVA EXTERNAL NAME 'tests.TestProcedures.removeQueryObserver'", SQL_NTS);
            DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode, "SQLExecDirect");

      /* --- execute DAPs for adding/removing observer --------------------------------------- */
      LOG("\tCall DAP Stmt= 'call PROC_ADD_QUERY_OBSRVR(2000)'");
      retcode = SQLExecDirect(hstmt, (SQLCHAR*)"call PROC_ADD_QUERY_OBSRVR(2000)", SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode, "SQLExecDirect");



      /* --- Query table --------------------------------------------- */
      LOG("\tSelect Stmt= 'SELECT name, age FROM SQLCANCEL2 where id > ?'");
      retcode = SQLPrepare(hstmt, (SQLCHAR*)"SELECT name, age FROM SQLCANCEL2 where id > ?", SQL_NTS);
      retcode = SQLBindParameter(hstmt, 1, SQL_PARAM_INPUT, SQL_C_LONG,
    		  SQL_INTEGER, 0, 0, &id, 0, NULL);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
                     "SQLBindParameter (SQL_INTEGER)");
      id = 1;

      retcode = SQLBindCol(hstmt, 1, SQL_C_CHAR, name, 100, NULL);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode, "SQLBindCol (SQL_CHAR)");

      retcode = SQLBindCol(hstmt, 2, SQL_C_LONG, &age, 0, NULL);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode, "SQLBindCol (SQL_INTEGER)");

      //creating cancel task for cancelling query
      ACE::init();
      LOG("Starting cancel task thread");
      LOGF("Main thread HSTMT id = %d", hstmt);
      // timed runs
      ACE_Barrier barrier(1);
      SQLCancelTask* task = new SQLCancelTask(barrier, hstmt);
      task->activate();
      barrier.wait();
      retcode = SQLExecute(hstmt);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_ERROR, retcode, "SQLExecute");

      if(retcode != SQL_ERROR)
      {
        do{
    	  retcode = SQLFetch(hstmt);
    	  LOGF("name = %s, age = %d", name, age);
    	  LOGF("retcode is %d", retcode);
        }while(retcode != SQL_NO_DATA && retcode != SQL_ERROR);

        DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode, "SQLFetch");
      }

      retcode = SQLFreeStmt(hstmt, SQL_RESET_PARAMS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode, "SQLFreeStmt");

      LOG("\tCall DAP Stmt= 'call PROC_REMOVE_QUERY_OBSRVR()'");
      retcode = SQLExecDirect(hstmt, (SQLCHAR*)"call PROC_REMOVE_QUERY_OBSRVR()", SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode, "SQLExecDirect");

      retcode = SQLFreeStmt(hstmt, SQL_CLOSE);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode, "SQLFreeStmt");
      /* ---------------------------------------------------------------------har- */

      /* --- Drop Table ----------------------------------------------- */
      LOGF("Drop Stmt= 'DROP TABLE SQLCANCEL2'");
      retcode = SQLExecDirect(hstmt, (SQLCHAR*)"DROP TABLE SQLCANCEL2", SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode, "SQLExecDirect");

      /* - Disconnect ---------------------------------------------------- */
      //free sql handles
      FREE_SQLHANDLES
}
END_TEST(testSQLCancelMT)


