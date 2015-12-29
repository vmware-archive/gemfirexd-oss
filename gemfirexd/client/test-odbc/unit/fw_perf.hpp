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
#ifndef _FWPERF_H_
#define _FWPERF_H_

/**

fw_perf.hpp provides framework macros for measuring performance,
archiving the results, and comparing against previous archives.

Steps for a performance suite:

perf::PerfSuite suite( "SuiteName" );

const char* name = "my operation type/test description";
perf::TimeStamp starttime;
... perform n operations.
perf::TimeStamp stoptime;
suite.addRecord( name, opcount, starttime, stoptime );

suite.save( );    // will save current results to <suite>_results.<hostname>
suite.compare( ); // will compare against the file named <suite>_baseline.<hostname>

If no baseline file for the host is available, then an error will occur, recommending
that the results be analyzed for acceptability, and checked in as the hosts baseline.

If a baseline is found, a comparison report is generated, if the deviation is beyond
established limits, a TestException will be thrown.

*/

#if defined(_MSC_VER)
/* 32 bit Windows only, for now */
typedef signed char      int8; /**< single byte, character or boolean field */
typedef unsigned char    uint8; /**< unsigned integer value */
typedef signed short     int16; /**< signed 16 bit integer (short) */
typedef unsigned short   uint16; /**< unsigned 16 bit integer (ushort) */
typedef signed int       int32; /**< signed 32 bit integer */
typedef unsigned int     uint32; /**< unsigned 32 bit integer */
typedef signed __int64   int64; /**< signed 64 bit integer */
typedef unsigned __int64 uint64; /**< unsigned 64 bit integer */

//typedef int32            intptr_t; /**< a pointer to a 32 bit integer */
//typedef uint32           uintptr_t; /**< a pointer to an unsigned 32 bit integer */

/* Windows does not have stdint.h */
typedef int8             int8_t;
typedef uint8            uint8_t;
typedef int16            int16_t;
typedef uint16           uint16_t;
typedef int32            int32_t;
typedef uint32           uint32_t;
typedef int64            int64_t;
typedef uint64           uint64_t;
/* end stdint.h */

/* Windows does not have inttypes.h */
/* 32 bit Windows only, for now */
#if !defined PRId8
# define PRId8 "d"
#endif
#if !defined PRIi8
# define PRIi8 "i"
#endif
#if !defined PRIo8
# define PRIo8 "o"
#endif
#if !defined PRIu8
# define PRIu8 "u"
#endif
#if !defined PRIx8
# define PRIx8 "x"
#endif
#if !defined PRIX8
# define PRIX8 "X"
#endif
#if !defined PRId16
# define PRId16 "d"
#endif
#if !defined PRIi16
# define PRIi16 "i"
#endif
#if !defined PRIo16
# define PRIo16 "o"
#endif
#if !defined PRIu16
# define PRIu16 "u"
#endif
#if !defined PRIx16
# define PRIx16 "x"
#endif
#if !defined PRIX16
# define PRIX16 "X"
#endif
#if !defined PRId32
# define PRId32 "d"
#endif
#if !defined PRIi32
# define PRIi32 "i"
#endif
#if !defined PRIo32
# define PRIo32 "o"
#endif
#if !defined PRIu32
# define PRIu32 "u"
#endif
#if !defined PRIx32
# define PRIx32 "x"
#endif
#if !defined PRIX32
# define PRIX32 "X"
#endif
#if !defined PRId64
# define PRId64 "lld"
#endif
#if !defined PRIi64
# define PRIi64 "lli"
#endif
#if !defined PRIo64
# define PRIo64 "llo"
#endif
#if !defined PRIu64
# define PRIu64 "llu"
#endif
#if !defined PRIx64
# define PRIx64 "llx"
#endif
#if !defined PRIX64
# define PRIX64 "llX"
#endif
/* end inttypes.h */

#ifndef _INC_WCHAR
#include <wchar.h>
#endif

#else
/* Unix, including both Sparc Solaris and Linux */
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
typedef int8_t           int8; /**< single byte, character or boolean field */
typedef uint8_t          uint8; /**< unsigned integer value */
typedef int16_t          int16; /**< signed 16 bit integer (short) */
typedef uint16_t         uint16; /**< unsigned 16 bit integer (ushort) */
typedef int32_t          int32; /**< signed 32 bit integer */
typedef uint32_t         uint32; /**< unsigned 32 bit integer */
typedef int64_t          int64; /**< signed 64 bit integer */
typedef uint64_t         uint64; /**< unsigned 64 bit integer */

#ifndef _WCHAR_H
#include <wchar.h>
#endif
#endif  /* _WIN32*/


#include <string>
#include <map>

//#include <gfcpp/DataOutput.hpp>
//#include <gfcpp/DataInput.hpp>

#include <ace/Task.h>
#include <ace/Condition_T.h>
#include <ace/Thread_Mutex.h>
//#include "Name_Handler.h"



namespace perf {

class Semaphore
{
  private:
    ACE_Thread_Mutex m_mutex;
    ACE_Condition< ACE_Thread_Mutex > m_cond;
    volatile int m_count;

  public:
    Semaphore( int count );
    ~Semaphore( );
    void acquire( int t = 1 );
    void release( int t = 1 );

  private:
    Semaphore( );
    Semaphore( const Semaphore& other );
    Semaphore& operator=( const Semaphore& other );
};

class TimeStamp
{
  private:

    int64_t m_msec;

  public:

    TimeStamp( );
    TimeStamp( const TimeStamp& other );
    TimeStamp( int64_t msec );
    TimeStamp& operator=( const TimeStamp& other );

    ~TimeStamp( );

    int64_t msec() const;
    void msec( int64_t t );
};

class Record
{
  private:
    std::string m_testName;
    int64_t m_operations;
    TimeStamp m_startTime;
    TimeStamp m_stopTime;

  public:
    Record( std::string testName, const long ops, const TimeStamp& start, const TimeStamp& stop );

    Record( );

    Record( const Record& other );

    Record& operator=( const Record& other );

   // void write( gemfire::DataOutput& output );

    //void read( gemfire::DataInput& input );

    int elapsed();
    int perSec();
    std::string asString();

    ~Record( );
};

typedef std::map< std::string, Record > RecordMap;

class PerfSuite
{
  private:
    std::string m_suiteName;
    RecordMap m_records;

  public:
    PerfSuite( const char* suiteName );

    void addRecord( std::string testName, const long ops, const TimeStamp& start, const TimeStamp& stop );

    /** create a file in cwd, named "<suite>_results.<host>" */
    void save( );

    /** load data saved in $ENV{'baselines'} named "<suite>_baseline.<host>" 
     *  A non-favorable comparison will throw an TestException.
     */
    void compare( );

};

class Thread;

class ThreadLauncher
{
  private:
    int m_thrCount;
    Semaphore m_initSemaphore;
    Semaphore m_startSemaphore;
    Semaphore m_stopSemaphore;
    Semaphore m_cleanSemaphore;
    Semaphore m_termSemaphore;
    TimeStamp* m_startTime;
    TimeStamp* m_stopTime;
    Thread& m_threadDef;

  public:
    ThreadLauncher( int thrCount, Thread& thr );

    void go();

    ~ThreadLauncher( );

    Semaphore& initSemaphore()
    {
      return m_initSemaphore;
    }

    Semaphore& startSemaphore()
    {
      return m_startSemaphore;
    }

    Semaphore& stopSemaphore()
    {
      return m_stopSemaphore;
    }

    Semaphore& cleanSemaphore()
    {
      return m_cleanSemaphore;
    }

    Semaphore& termSemaphore()
    {
      return m_termSemaphore;
    }

    TimeStamp startTime( ) 
    {
      return *m_startTime;
    }

    TimeStamp stopTime( )
    {
      return *m_stopTime;
    }

  private:
    ThreadLauncher& operator=( const ThreadLauncher& other );
    ThreadLauncher( const ThreadLauncher& other );
};

class Thread
: public ACE_Task_Base
{
  private:

    ThreadLauncher* m_launcher;
    bool m_used;

  public:

    Thread( );
    //Unhide function to prevent SunPro Warnings
    using ACE_Shared_Object::init;
    void init( ThreadLauncher* l ) 
    {
      ASSERT( ! m_used, "Cannot reliably reuse Thread." ); 
      m_launcher = l;
    }

    ~Thread( );

    /** called before measurement begins. override to do per thread setup. */
    virtual void setup( )
    {
    }

    /** run during measurement */
    virtual void perftask( ) = 0; 

    /** called after measurement to clean up what might have been setup in setup.. */
    virtual void cleanup( )
    {
    }

    virtual int svc( );

};

//class NamingServiceThread
//: public ACE_Task_Base
//{
//  private:
//  uint32_t m_port;
//  
//  void namingService()
//  {
//    char * argsv[2];
//    char pbuf[32];
//    sprintf( pbuf, "-p %d", 12321 );
//  
//    argsv[0] = strdup( pbuf );
//    argsv[1] = 0;
//    ACE_Service_Object_Ptr svcObj = ACE_SVC_INVOKE( ACE_Name_Acceptor );
//  
//    if ( svcObj->init( 1, argsv ) == -1 ) {
//      fprintf( stdout, "Failed to construct the Naming Service." );
//      fflush( stdout );
//    }
//      ACE_Reactor::run_event_loop();
//  }
//    
//  public:
//  NamingServiceThread( uint32_t port ) : m_port( port ) {}
//virtual int svc() { };//namingService(); }
//};

}

#endif
