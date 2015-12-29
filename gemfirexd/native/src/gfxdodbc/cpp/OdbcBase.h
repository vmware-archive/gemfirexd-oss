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

/**
 * OdbcBase.h
 *
 * Base includes, types, macros used by the ODBC driver.
 *
 *      Author: swale
 */

#ifndef ODBCBASE_H_
#define ODBCBASE_H_

/** ODBC 3.51 conformance */
#define ODBCVER 0x0351

#if defined(WIN32) || defined(__MINGW32__)
#  ifndef _WIN32
#    define _WIN32 1
#  endif
#endif
#if defined(WIN64) || defined(__MINGW64__)
#  ifndef _WIN64
#    define _WIN64 1
#  endif
#endif
#if defined(_WIN32) || defined(_WIN64)
#  ifndef _WINDOWS
#    define _WINDOWS 1
#  endif
#endif

#if defined(__linux__) || defined(__linux)
#  ifndef _LINUX
#    define _LINUX 1
#  endif
#endif

#ifdef __sun
#  ifndef _SOLARIS
#    define _SOLARIS 1
#  endif
#endif

#ifdef __APPLE__
#  ifndef _MACOSX
#    define _MACOSX 1
#  endif
#endif

#ifdef __FreeBSD__
#  ifndef _FREEBSD
#    define _FREEBSD 1
#  endif
#endif

/* define the macro for DLL export/import on windows */
#ifdef _WINDOWS
#  define DLLEXPORT __declspec(dllexport)
#  define DLLIMPORT __declspec(dllimport)
#  ifdef DLLBUILD
#    define DLLPUBLIC DLLEXPORT
#  else
#    define DLLPUBLIC DLLIMPORT
#  endif
extern "C" {
#  include <windows.h>
#  include <process.h>
}
#  define LOCK HANDLE
#else
#  define DLLPUBLIC
extern "C" {
#  include <pthread.h>
#  include <errno.h>
#  include <dlfcn.h>
}
#  define LOCK pthread_mutex_t
//#  define SQL_WCHART_CONVERT
#endif

#ifdef _MACOSX
#if defined(i386) || defined(__i386) || defined(__i386__)
#define SIZEOF_LONG_INT 4 
#endif
#endif

extern "C" {
#include <sql.h>
#include <sqlext.h>
}

#endif /* ODBCBASE_H_ */
