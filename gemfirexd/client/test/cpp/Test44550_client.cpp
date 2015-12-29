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
#include <iostream>
#include <sstream>
#include <string>

#include <ace/ACE.h>
#undef ACE_SCANDIR_CMP_USES_VOIDPTR
#include <ace/OS.h>
#include <ace/Task.h>
#include <ace/Barrier.h>

//#include <boost/lexical_cast.hpp>

#include "GFXDClient.h"

extern "C"
{
#  include <stdio.h>
}

using namespace com::pivotal::gemfirexd;
using namespace com::pivotal::gemfirexd::client;

extern "C"
{
  extern void ProfilerEnable();
}

class SelectTask: public ACE_Task_Base
{
private:
  std::string& m_host;
  int m_port;
  ACE_Barrier& m_barrier;
  const int m_numRows;
  const int m_numRuns;

public:
  inline SelectTask(std::string& host, int port, ACE_Barrier& barrier,
      int numRows, int numRuns) :
      m_host(host), m_port(port), m_barrier(barrier), m_numRows(numRows), m_numRuns(
          numRuns)
  {
  }

  int svc()
  {
    AutoPtr<PreparedStatement> pstmt;
    Parameters params;
    AutoPtr<ResultSet> rs;
    ResultSet::const_iterator rsIter;

    int status = 0;

    try {
      Connection conn;
      std::map<std::string, std::string> props;
      //props["binary-protocol"] = "true";
      conn.open(m_host, m_port, props);
      pstmt = conn.prepareStatement("SELECT * FROM new_order "
          "WHERE no_d_id = ? AND no_w_id = ? AND no_o_id = ?");

      std::ostringstream msg;
      msg << "Starting timed selects for thread " << ACE_OS::thr_self() << '\n';
      std::cout << msg.str();

      // wait for all threads
      m_barrier.wait();

      int rowNum, w_id;

      for (int i = 1; i <= m_numRuns; i++) {
        rowNum = (i % m_numRows) + 1;
        w_id = (rowNum % 98);
        params.addInt(w_id).addInt(w_id).addInt(rowNum);

        rs = pstmt->executeQuery(params);

        int numResults = 0;
        for (rsIter = rs->begin(); rsIter != rs->end(); ++rsIter) {
          rsIter->getInt(1);
          //rsIter->getString(2);
          numResults++;
        }
        rs->close();
        if (numResults == 0) {
          std::cerr << "unexpected 0 results for w_id, d_id " << w_id
              << std::endl;
          status = -1;
        }
        params.clear();
      }
    } catch (SQLException& sqle) {
      std::cerr << "Failed with " << stack(sqle);
      const SQLException* next = sqle.getNextException();
      if (next != NULL) {
        std::cerr << *next;
      }
      std::cerr << std::endl;
    } catch (std::exception& ex) {
      std::cerr << "ERROR: failed with exception: " << ex << std::endl;
    } catch (...) {
      std::cerr << "ERROR: failed with unknown exception..." << std::endl;
    }
    return status;
  }
};

/**
 * Simple performance test given n threads of execution.
 */
int main(int argc, char** argv)
{
  if (argc != 3 && argc != 4) {
    std::cerr << "Usage: <script> <server> <port> [<threads>]\n";
    return 1;
  }
  std::string server = argv[1];
  //int port = boost::lexical_cast<int>(argv[2]);
  int port = ::atoi(argv[2]);
  int numThreads = 1;
  if (argc == 4) {
    numThreads = ACE_OS::atoi(argv[3]);
    if (numThreads <= 0) {
      std::cerr << "unexpected number of threads " << numThreads << "\n";
      return 1;
    }
  }

  ACE_Time_Value start, end;
  time_t startSecs, endSecs;
  suseconds_t startUSecs, endUSecs;
  SelectTask** tasks = NULL;
  //ProfilerEnable();
  try {
    Connection conn;
    AutoPtr<PreparedStatement> pstmt;
    Parameters params;
    AutoPtr<ResultSet> rs;
    ResultSet::const_iterator rsIter;

    //SystemProperties::setProperty(ClientProperty::LOG_FILE_NAME, "thrift-client.log");
    //SystemProperties::setProperty(ClientProperty::LOG_LEVEL_NAME, "config");

    if (LogWriter::INFO_ENABLED()) {
      LogWriter::INFO() << "Connecting to " << server << ':' << port << LogWriter::NEWLINE;
    }
    std::map<std::string, std::string> props;
    //props["binary-protocol"] = "true";
    //props["log-file"] = "thrift-client.log";
    conn.open(server, port, props);

    conn.executeUpdate("drop table if exists new_order");
    conn.executeUpdate("drop table if exists customer");

    // create the tables
    conn.executeUpdate("create table customer ("
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
        ") partition by (c_w_id) redundancy 1");
    conn.executeUpdate("create table new_order ("
        "no_w_id  integer   not null,"
        "no_d_id  integer   not null,"
        "no_o_id  integer   not null,"
        "no_name  varchar(100) not null"
        ") partition by (no_w_id) colocate with (customer) redundancy 1");
    conn.executeUpdate("alter table customer add constraint pk_customer "
        "primary key (c_w_id, c_d_id, c_id)");
    conn.executeUpdate("create index ndx_customer_name "
        "on customer (c_w_id, c_d_id, c_last)");
    conn.executeUpdate("alter table new_order add constraint pk_new_order "
        "primary key (no_w_id, no_d_id, no_o_id)");
    conn.executeUpdate("create index ndx_neworder_w_id_d_id "
        "on new_order (no_w_id, no_d_id)");
    conn.execute("create index ndx_neworder_w_id_d_id_o_id "
        "on new_order (no_w_id, no_d_id, no_o_id)");

    std::cout << "Created tables\n";
    std::cout << "Will use " << numThreads << " threads for selects\n";

    const int numRows = 10000;

    pstmt = conn.prepareStatement("insert into new_order values "
        "(?, ?, ?, ?)");

    int id, w_id, count;
    char name[100];

    std::cout << "Starting inserts\n";
    for (id = 1; id <= numRows; id++) {
      w_id = (id % 98);
      ::sprintf(name, "customer-with-order%d%d", id, w_id);
      params.addInt(w_id).addInt(w_id).addInt(id).addString(name);

      count = pstmt->executeUpdate(params);
      if (count != 1) {
        std::cerr << "unexpected count for single insert: " << count << "\n";
        return 2;
      }
      params.clear();
      if ((id % 500) == 0) {
        std::cout << "Completed " << id << " inserts ...\n";
      }
    }

    pstmt = conn.prepareStatement("SELECT * FROM new_order "
        "WHERE no_d_id = ? AND no_w_id = ? AND no_o_id = ?");

    std::cout << "Starting warmup selects\n";
    const int numRuns = 50000;
    int rowNum;

    // warmup for the selects
    for (int i = 1; i <= numRuns; i++) {
      rowNum = (i % numRows) + 1;
      w_id = (rowNum % 98);
      params.addInt(w_id).addInt(w_id).addInt(rowNum);

      rs = pstmt->executeQuery(params);

      int numResults = 0;
      for (rsIter = rs->begin(); rsIter != rs->end(); ++rsIter) {
        rsIter->getInt(1);
        rsIter->getString(2);
        numResults++;
      }
      rs->close();
      params.clear();
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

    if (numThreads > 1) {
      // create the other threads
      tasks = new SelectTask*[numThreads - 1];
      for (int i = 0; i < (numThreads - 1); i++) {
        tasks[i] = new SelectTask(server, port, barrier, numRows, numRuns);
        tasks[i]->activate();
      }
    }
    barrier.wait();
    start = ACE_OS::gettimeofday();
    for (int i = 1; i <= numRuns; i++) {
      rowNum = (i % numRows) + 1;
      w_id = (rowNum % 98);
      params.addInt(w_id).addInt(w_id).addInt(rowNum);

      rs = pstmt->executeQuery(params);

      int numResults = 0;
      for (rsIter = rs->begin(); rsIter != rs->end(); ++rsIter) {
        rsIter->getInt(1);
        //rsIter->getString(2);
        numResults++;
      }
      rs->close();
      params.clear();
      if (numResults == 0) {
        std::cerr << "unexpected 0 results for w_id, d_id " << w_id << "\n";
      }
    }
  } catch (SQLException& sqle) {
    std::cerr << "Failed with " << stack(sqle);
    const SQLException* next = sqle.getNextException();
    if (next != NULL) {
      std::cerr << *next;
    }
    std::cerr << std::endl;
  } catch (std::exception& ex) {
    std::cerr << "ERROR: failed with exception: " << ex << std::endl;
  } catch (...) {
    std::cerr << "ERROR: failed with unknown exception..." << std::endl;
  }

  if (numThreads > 1 && tasks != NULL) {
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
  if (numThreads > 1 && tasks != NULL) {
    for (int i = 0; i < (numThreads - 1); i++) {
      delete tasks[i];
    }
    delete [] tasks;
  }

  ACE::fini();

  return 0;
}
