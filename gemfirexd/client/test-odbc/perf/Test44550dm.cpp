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
 
// uses DriverManager to load the driver

#include <iostream>
#include <string>

#include <ace/ACE.h>
#undef ACE_SCANDIR_CMP_USES_VOIDPTR
#include <ace/OS.h>
#include <ace/Task.h>
#include <ace/Barrier.h>

extern "C"
{
#include <sql.h>
#include <sqlext.h>
#include <stdlib.h>
}

#define DSN "gemfirexd"

void printStatementError(SQLHSTMT stmt, int line)
{
  SQLCHAR sqlState[6];
  SQLINTEGER errorCode;
  SQLCHAR message[8192];
  SQLSMALLINT messageLen;
  ::SQLGetDiagRec(SQL_HANDLE_STMT, stmt, 1, sqlState, &errorCode, message,
      8191, &messageLen);
  std::cout << "Statement before line " << line << " failed with SQLState("
      << sqlState << ", ErrorCode=" << errorCode << ": " << message << "\n";
}

class SelectTask: public ACE_Task_Base
{
private:
  SQLHENV m_env;
  std::string& m_connStr;
  ACE_Barrier& m_barrier;
  int m_numRuns;

public:
  inline SelectTask(SQLHENV env, std::string& connStr, ACE_Barrier& barrier,
      int numRuns) :
      m_env(env), m_connStr(connStr), m_barrier(barrier), m_numRuns(numRuns)
  {
  }

  int svc()
  {
    SQLHDBC conn;
    SQLHSTMT stmt;

    ::SQLAllocHandle(SQL_HANDLE_DBC, m_env, &conn);

    if (!SQL_SUCCEEDED(::SQLConnect(conn, (SQLCHAR*)DSN, sizeof(DSN) - 1,
            NULL, 0, NULL, 0))) {
      // connection failed
      SQLCHAR sqlState[6];
      SQLINTEGER errorCode;
      SQLCHAR message[8192];
      SQLSMALLINT messageLen;
      ::SQLGetDiagRec(SQL_HANDLE_DBC, conn, 1, sqlState, &errorCode, message,
          8191, &messageLen);
      std::cout << "Connection failed for thread "
          << ACE_OS::thr_self() + " with SQLState(" << sqlState
          << ", ErrorCode=" << errorCode << ": " << message << "\n";
      return -1;
    }

    ::SQLAllocHandle(SQL_HANDLE_STMT, conn, &stmt);

    ::SQLPrepare(stmt, (SQLCHAR*)"SELECT no_o_id FROM new_order "
        "WHERE no_d_id = ? AND no_w_id = ? ORDER BY no_o_id ASC", SQL_NTS);

    std::cout << "Starting timed selects for thread " << ACE_OS::thr_self()
        << "\n";

    // wait for all threads
    m_barrier.wait();

    int status = 0;
    for (int i = 1; i <= m_numRuns; i++) {
      int w_id = (i % 98);
      ::SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER, 0,
          0, &w_id, sizeof(w_id), NULL);
      ::SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER, 0,
          0, &w_id, sizeof(w_id), NULL);
      ::SQLExecute(stmt);

      int numResults = 0;
      while (SQL_SUCCEEDED(::GFXDetch(stmt))) {
        int o_id;
        ::SQLGetData(stmt, 1, SQL_C_LONG, &o_id, sizeof(o_id), NULL);
        numResults++;
      }
      ::SQLCloseCursor(stmt);
      if (numResults == 0) {
        std::cerr << "unexpected 0 results for w_id, d_id " << w_id << "\n";
        status = -1;
      }
    }
    return status;
  }
};

/**
 * Simple performance test given n threads of execution.
 */
int main(int argc, char** argv)
{
  int numThreads = 1;

  if (argc != 1 && argc != 2) {
    std::cerr << "Usage: <script> [<threads>]\n";
    return 1;
  }
  if (argc == 2) {
    numThreads = ACE_OS::atoi(argv[1]);
    if (numThreads <= 0) {
      std::cerr << "unexpected number of threads " << numThreads << "\n";
      return 1;
    }
  }

  // set the environment variables for ini files
  ::setenv("ODBCINI", ".", 1);
  ::setenv("ODBCSYSINI", ".", 1);

  std::string connStr;
  connStr.append("DSN=" DSN ";UID=app;PWD=app");

  SQLHENV env;
  SQLHDBC conn;
  SQLHSTMT stmt;
  SQLCHAR outConnStr[8192];

  if (!SQL_SUCCEEDED(::SQLAllocHandle(SQL_HANDLE_ENV, NULL, &env))) {
    SQLCHAR sqlState[6];
    SQLINTEGER errorCode;
    SQLCHAR message[8192];
    SQLSMALLINT messageLen;
    ::SQLGetDiagRec(SQL_HANDLE_ENV, env, 1, sqlState, &errorCode, message,
        8191, &messageLen);
    std::cout << "Initialization failed with SQLState(" << sqlState
      << ", ErrorCode=" << errorCode << ": " << message << "\n";
    return 1;
  }

  if (!SQL_SUCCEEDED(::SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION,
          (void*)SQL_OV_ODBC3, 0))) {
    SQLCHAR sqlState[6];
    SQLINTEGER errorCode;
    SQLCHAR message[8192];
    SQLSMALLINT messageLen;
    ::SQLGetDiagRec(SQL_HANDLE_ENV, env, 1, sqlState, &errorCode, message,
        8191, &messageLen);
    std::cout << "Initialization failed with SQLState(" << sqlState
      << ", ErrorCode=" << errorCode << ": " << message << "\n";
    return 1;
  }

  ::SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  std::cout << "Connecting using connection string: " << connStr << "\n";
  if (!SQL_SUCCEEDED(::SQLDriverConnect(conn, NULL, (SQLCHAR*)connStr.c_str(),
      connStr.size(), outConnStr, sizeof(outConnStr), NULL, SQL_DRIVER_NOPROMPT))) {
    // connection failed
    SQLCHAR sqlState[6];
    SQLINTEGER errorCode;
    SQLCHAR message[8192];
    SQLSMALLINT messageLen;
    ::SQLGetDiagRec(SQL_HANDLE_DBC, conn, 1, sqlState, &errorCode, message,
        8191, &messageLen);
    std::cout << "Connection failed with SQLState(" << sqlState
      << ", ErrorCode=" << errorCode << ": " << message << "\n";
    return 1;
  }
  std::cout << "Connected using connection string: " << outConnStr << "\n";

  ::SQLAllocHandle(SQL_HANDLE_STMT, conn, &stmt);

  // create the tables
  ::SQLExecDirect(stmt, (SQLCHAR*)"create table customer ("
    "c_w_id         integer        not null,"
    "c_d_id         integer        not null,"
    "c_id           integer        not null,"
    "c_discount     decimal(4,4),"
    "c_credit       char(2),"
    "c_last         varchar(16),"
    "c_first        varchar(16),"
    "c_credit_lim   decimal(12,2),"
    "c_balance      decimal(12,2),"
    "c_ytd_payment  float,"
    "c_payment_cnt  integer,"
    "c_delivery_cnt integer,"
    "c_street_1     varchar(20),"
    "c_street_2     varchar(20),"
    "c_city         varchar(20),"
    "c_state        char(2),"
    "c_zip          char(9),"
    "c_phone        char(16),"
    "c_since        timestamp,"
    "c_middle       char(2),"
    "c_data         varchar(500)"
    ") partition by (c_w_id) redundancy 1", SQL_NTS);
  ::SQLExecDirect(stmt, (SQLCHAR*)"create table new_order ("
    "no_w_id  integer   not null,"
    "no_d_id  integer   not null,"
    "no_o_id  integer   not null"
    ") partition by (no_w_id) colocate with (customer) redundancy 1", SQL_NTS);
  ::SQLExecDirect(stmt,
      (SQLCHAR*)"alter table customer add constraint pk_customer "
        "primary key (c_w_id, c_d_id, c_id)", SQL_NTS);
  ::SQLExecDirect(stmt, (SQLCHAR*)"create index ndx_customer_name "
    "on customer (c_w_id, c_d_id, c_last)", SQL_NTS);
  ::SQLExecDirect(stmt,
      (SQLCHAR*)"alter table new_order add constraint pk_new_order "
        "primary key (no_w_id, no_d_id, no_o_id)", SQL_NTS);
  ::SQLExecDirect(stmt, (SQLCHAR*)"create index ndx_neworder_w_id_d_id "
    "on new_order (no_w_id, no_d_id)", SQL_NTS);
  ::SQLExecDirect(stmt, (SQLCHAR*)"create index ndx_neworder_w_id_d_id_o_id "
    "on new_order (no_w_id, no_d_id, no_o_id)", SQL_NTS);

  std::cout << "Created tables\n";
  std::cout << "Will use " << numThreads << " threads for selects\n";

  const int numRows = 10000;

  if (!SQL_SUCCEEDED(::SQLPrepare(stmt, (SQLCHAR*)"insert into new_order values (?, ?, ?)",
      SQL_NTS))) {
    printStatementError(stmt, __LINE__);
    return 2;
  }

  std::cout << "Starting inserts\n";
  for (int id = 1; id <= numRows; id++) {
    int w_id = (id % 98);
    ::SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER, 0, 0,
        &w_id, sizeof(w_id), NULL);
    ::SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER, 0, 0,
        &w_id, sizeof(w_id), NULL);
    ::SQLBindParameter(stmt, 3, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER, 0, 0,
        &id, sizeof(id), NULL);
    if (!SQL_SUCCEEDED(::SQLExecute(stmt))) {
      printStatementError(stmt, __LINE__);
      return 2;
    }

    SQLLEN count;
    if (!SQL_SUCCEEDED(::SQLRowCount(stmt, &count))) {
      printStatementError(stmt, __LINE__);
      return 2;
    }
    if (count != 1) {
      std::cerr << "unexpected count for single insert: " << count << "\n";
      return 2;
    }
    if ((id % 500) == 0) {
      std::cout << "Completed " << id << " inserts ...\n";
    }
  }

  ::SQLPrepare(stmt, (SQLCHAR*)"SELECT no_o_id FROM new_order "
    "WHERE no_d_id = ? AND no_w_id = ? ORDER BY no_o_id ASC", SQL_NTS);

  std::cout << "Starting warmup selects\n";
  const int numRuns = 50000;
  // warmup for the selects
  for (int i = 1; i <= numRuns; i++) {
    int w_id = (i % 98);
    ::SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER, 0, 0,
        &w_id, sizeof(w_id), NULL);
    ::SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER, 0, 0,
        &w_id, sizeof(w_id), NULL);
    if (!SQL_SUCCEEDED(::SQLExecute(stmt))) {
      printStatementError(stmt, __LINE__);
      return 2;
    }

    int numResults = 0;
    while (SQL_SUCCEEDED(::GFXDetch(stmt))) {
      int o_id;
      ::SQLGetData(stmt, 1, SQL_C_LONG, &o_id, sizeof(o_id), NULL);
      numResults++;
    }
    ::SQLCloseCursor(stmt);
    if (numResults == 0) {
      std::cerr << "unexpected 0 results for w_id, d_id " << w_id << "\n";
      return 2;
    }
    if ((i % 500) == 0) {
      std::cout << "Completed " << i << " warmup selects ...\n";
    }
  }

  ACE::init();
  std::cout << "Starting timed selects with " << numThreads << " threads\n";
  // timed runs
  ACE_Barrier barrier(numThreads);
  ACE_Time_Value start, end;
  time_t startSecs, endSecs;
  suseconds_t startUSecs, endUSecs;
  SelectTask** tasks = NULL;

  if (numThreads > 1) {
    // create the other threads
    tasks = new SelectTask*[numThreads - 1];
    for (int i = 0; i < (numThreads - 1); i++) {
      tasks[i] = new SelectTask(env, connStr, barrier, numRuns);
      tasks[i]->activate();
    }
  }
  barrier.wait();
  start = ACE_OS::gettimeofday();
  for (int i = 1; i <= numRuns; i++) {
    int w_id = (i % 98);
    ::SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER, 0, 0,
        &w_id, sizeof(w_id), NULL);
    ::SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER, 0, 0,
        &w_id, sizeof(w_id), NULL);
    ::SQLExecute(stmt);

    int numResults = 0;
    while (SQL_SUCCEEDED(::GFXDetch(stmt))) {
      int o_id;
      ::SQLGetData(stmt, 1, SQL_C_LONG, &o_id, sizeof(o_id), NULL);
      numResults++;
    }
    ::SQLCloseCursor(stmt);
    if (numResults == 0) {
      std::cerr << "unexpected 0 results for w_id, d_id " << w_id << "\n";
    }
  }
  if (numThreads > 1) {
    // wait for other threads to join
    for (int i = 0; i < (numThreads - 1); i++) {
      tasks[i]->wait();
    }
  }
  end = ACE_OS::gettimeofday();

  startSecs = start.sec();
  startUSecs = start.usec();
  endSecs = end.sec();
  endUSecs = end.usec();

  if (startUSecs > endUSecs) {
    endSecs--;
    endUSecs += 1000000;
  }
  std::cout << "Time taken: " << (endSecs - startSecs) << '.'
    << (endUSecs - startUSecs) << "s\n";

  // cleanup
  if (numThreads > 1) {
    for (int i = 0; i < (numThreads - 1); i++) {
      delete tasks[i];
    }
    delete tasks;
  }

  ACE::fini();

  ::GFXDreeHandle(SQL_HANDLE_STMT, stmt);
  ::SQLDisconnect(conn);
  ::GFXDreeHandle(SQL_HANDLE_DBC, conn);
  ::GFXDreeHandle(SQL_HANDLE_ENV, env);

  return 0;
}
