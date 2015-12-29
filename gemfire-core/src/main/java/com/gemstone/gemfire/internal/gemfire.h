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
 
#ifndef GEMFIRE_H
#define GEMFIRE_H
#if defined(_WIN32)
/** Library Export */
#define LIBEXP __declspec(dllexport)
/** Library Implementation */
#define LIBIMP __declspec(dllimport)
/** Library Call */ 
#define LIBCALL __stdcall
/** Library Export a type */
#define LIBEXPORT(type) LIBEXP type LIBCALL
#else
/** Library Export */
#define LIBEXP
/** Library Implementation */
#define LIBIMP extern
/** Library Call */
#define LIBCALL /**/
/** Library Export a type */
#define LIBEXPORT(type) type
#endif

/** Defines a GemFire C extern */
#ifdef BUILD_GEMFIRE
#define GFCEXTERN(type) LIBEXP type LIBCALL
#else
#define GFCEXTERN(type) LIBIMP type LIBCALL
#endif /* BUILD_GEMFIRE    */

#if defined(_WIN32)
/* 32 bit Windows only, for now */
#include <windows.h>
typedef char             int8; /**< single byte, character or boolean field */
typedef unsigned char    uint8; /**< unsigned integer value */
typedef short            int16; /**< signed 16 bit integer (short) */
typedef unsigned short   uint16; /**< unsigned 16 bit integer (ushort) */
typedef int              int32; /**< signed 32 bit integer */
typedef unsigned int     uint32; /**< unsigned 32 bit integer */
typedef __int64          int64; /**< signed 64 bit integer */
typedef unsigned __int64 uint64; /**< unsigned 64 bit integer */

#if (_MSC_VER <= 1200)
typedef int32            intptr_t; /**< a pointer to a 32 bit integer */
typedef uint32           uintptr_t; /**< a pointer to an unsigned 32 bit integer */
#endif

#ifndef _INC_WCHAR
#include <wchar.h>
#endif

#else
/* Unix, including both Sparc Solaris and Linux */
#include <inttypes.h>
typedef char             int8; /**< single byte, character or boolean field */
typedef unsigned char    uint8; /**< unsigned integer value */
typedef short            int16; /**< signed 16 bit integer (short) */
typedef unsigned short   uint16; /**< unsigned 16 bit integer (ushort) */
typedef int32_t          int32; /**< signed 32 bit integer */
typedef uint32_t         uint32; /**< unsigned 32 bit integer */
typedef int64_t          int64; /**< signed 64 bit integer */
typedef uint64_t         uint64; /**< unsigned 64 bit integer */

#ifndef _WCHAR_H
#include <wchar.h>
#endif
#endif  /* _WIN32*/ 


/** @mainpage GemFire C Reference
 * @image html gemFireLogo2.gif
 *
*/

/**
 * @file
 *
 *  Definitions of types and functions supported in the GemFire C Internface 
 */

typedef uint32  Oid; /**< See @ref gfoids.h for a list of predefined objectIds */

/**
 * Status values returned by gfConnectionStatus
 */
enum {
  GF_S_NOTCON = 0,     /**< connection not yet opened            */
  GF_S_CONN = 1,       /**< connected                            */
  GF_S_DISCON = 2,     /**< disconnected with gfDisconnect       */
  GF_S_SHUTDN = 3      /**< disconnected due to manager shutdown */
};

/** boolean value FALSE */
#define GF_FALSE 0  
/** boolean value TRUE */
#define GF_TRUE  1  

/** GemFire shared memory unprotected */
#define GF_UNPROTECT 0
/** GemFire shared memory protected */
#define GF_PROTECT 1

/** 
 * @enum GfErrType 
 *Error codes returned by GemFire C Interface functions
 */
typedef enum {
  GF_NOERR = 0,    /**< success - no error               */
  GF_DEADLK = 1,   /**< deadlock detected                */
  GF_EACCES = 2,   /**< permission problem               */
  GF_ECONFL = 3,   /**< class creation conflict          */
  GF_EINVAL = 4,   /**< invalid argument                 */
  GF_ENOENT = 5,   /**< entity does not exist            */
  GF_ENOMEM = 6,   /**< insufficient memory              */
  GF_ERANGE = 7,   /**< index out of range               */
  GF_ETYPE =  8,   /**< type mismatch                    */
  GF_NOTOBJ = 9,   /**< invalid object reference         */
  GF_NOTCON = 10,  /**< not connected to gemfire         */
  GF_NOTOWN = 11,  /**< lock not owned by process/thread */
  GF_NOTSUP = 12,  /**< operation not supported          */
  GF_SCPGBL = 13,  /**< attempt to exit global scope     */
  GF_SCPEXC = 14,  /**< maximum scopes exceeded          */
  GF_TIMOUT = 15,  /**< operation timed out              */
  GF_OVRFLW = 16,  /**< arithmetic overflow 	         */
  GF_IOERR =  17,  /**< paging file I/O error            */
  GF_EINTR =  18,  /**< interrupted gemfire call         */

  GF_CACHE_REGION_NOT_FOUND = 101, /**< No region with the specified name. */
  GF_CACHE_REGION_INVALID =   102, /**< the region is not valid */
  GF_CACHE_REGION_KEYS_NOT_STRINGS = 103,/**< Entry keys are not strings */
  GF_CACHE_REGION_ENTRY_NOT_BYTES = 104, /**< Entry's value is not a byte array */
  GF_CACHE_REGION_NOT_GLOBAL = 105,      /**< Distributed locks not supported */
  GF_CACHE_PROXY = 106,                  /**< Errors detected in CacheProxy processing */
  GF_CACHE_ILLEGAL_ARGUMENT_EXCEPTION = 107, /**< IllegalArgumentException in Cache Proxy */
  GF_CACHE_ILLEGAL_STATE_EXCEPTION = 108, /**< IllegalStateException in CacheProxy */
  GF_CACHE_TIMEOUT_EXCEPTION = 109, /**< TimeoutException in CacheProxy */ 
  GF_CACHE_WRITER_EXCEPTION = 110, /**< CacheWriterException in CacheProxy */
  GF_CACHE_REGION_EXISTS_EXCEPTION = 111, /**< RegionExistsException in CacheProxy */
  GF_CACHE_CLOSED_EXCEPTION = 112, /**< CacheClosedException in CacheProxy */
  GF_CACHE_LEASE_EXPIRED_EXCEPTION = 113, /**< LeaseExpiredException in CacheProxy */
  GF_CACHE_LOADER_EXCEPTION = 114, /**< CacheLoaderException in CacheProxy */
  GF_CACHE_REGION_DESTROYED_EXCEPTION = 115, /**< RegionDestroyedException in CacheProxy */
  GF_CACHE_ENTRY_DESTROYED_EXCEPTION = 116, /**< EntryDestroyedException in CacheProxy */
  GF_CACHE_STATISTICS_DISABLED_EXCEPTION = 117, /**< StatisticsDisabledException in CacheProxy */
  GF_CACHE_CONCURRENT_MODIFICATION_EXCEPTION = 118, /**< ConcurrentModificationException in CacheProxy */
  GF_CACHE_ENTRY_NOT_FOUND = 119, /**< EntryNotFoundException in CacheProxy */
  GF_CACHE_ENTRY_EXISTS = 120, /**< EntryExistsException in CacheProxy */

  GF_EUNDEF = 999  /**< unknown exception                */
} GfErrType;


/** 
 * @enum GfFieldType 
 * Identifiers for field types used to define fields in classes. 
 */
typedef enum { /* values must agree with FieldInfo.java*/
  GF_FIELDTYPE_OBJECT = 0,   /**< Object */
  GF_FIELDTYPE_BOOLEAN = 1,  /**< Boolean */
  GF_FIELDTYPE_CHAR = 2,     /**< Character */
  GF_FIELDTYPE_BYTE = 3,     /**< Byte */
  GF_FIELDTYPE_SHORT = 4,    /**< Short */
  GF_FIELDTYPE_INT = 5,      /**< Int */
  GF_FIELDTYPE_LONG = 6,     /**< Long */
  GF_FIELDTYPE_FLOAT = 7,    /**< Float */
  GF_FIELDTYPE_DOUBLE = 8,   /**< Double */
  GF_FIELDTYPE_UNSIGNED_BYTE = 9, /**< UnsignedByte */
  GF_FIELDTYPE_STRING = 10,  /**< String */
  GF_FIELDTYPE_WSTRING = 11, /**< Wide String*/
  GF_FIELDTYPE_WCHAR = 12    /**< Wide Character */
} GfFieldType;

/**
 * @enum GfSharingType
 * Values for "sharing" field in shared class
 */
typedef enum {
  GF_SHARING_TYPE_DIRECT = 0, /**< direct sharing */
  GF_SHARING_TYPE_COPY = 1    /**< copysharing */
} GfSharingType;
   
/** SystemInfo string size */
#define SYS_INFO_STR_SIZE 256 
   
/**
 * @struct GfSysInfo
 *Structure used to return system information
 */
typedef struct {
  char connectionName[SYS_INFO_STR_SIZE]; /**<  The name of the current connection */
  char versionString[SYS_INFO_STR_SIZE];  /**<  The GemFire version string */
  char systemDir[SYS_INFO_STR_SIZE];      /**<  The system directory path */
} GfSysInfo;


/** GemFire maximum error string length */
#define GF_MAX_ERR_STR_LEN  256 

/** 
 * @struct GfErrorInfo
 * Structure used to return error information 
 */
typedef struct {
    GfErrType errorno; /**< The error number (error code) */
    char errorString[GF_MAX_ERR_STR_LEN]; /**< Text describing the error condion */
    int32 numArgs; /**< The number of arguments that were included */
    Oid args[4]; /**< A list of arguments included with the error */
} GfErrorInfo;

enum { GFMSG_MNAME_SIZE = 128 };  /* see SharedMethodInvocation.java */
enum {  GFMSG_MAX_ARGS = 10 };    /* see SharedMethodInvocation.java */

/**
 * @struct GfListenArgType
 * Defines fields for Listener arguments
 */
typedef struct {
  char methodName[GFMSG_MNAME_SIZE]; /**< The name of the method to execute */
  Oid sharedMethodInvocationObj;     /**< The shared memory object for communication */
  int32 numArgs;                     /**< The number of arguments */
  Oid args[GFMSG_MAX_ARGS];          /**< The list of arguments */  
} GfListenArgType;

/**
 * @struct GfNotifyListenerArgType
 * Defines fields for Listener notification
 */
typedef struct {
  char methodName[GFMSG_MNAME_SIZE]; /**< The name of the method to execute */
  int32 numArgs;                     /**< The number of arguments */
  Oid args[GFMSG_MAX_ARGS];          /**< The list of arguments */  
  Oid methodInvocationObj;           /**< The methodInvocation object */
} GfNotifyListenerArgType;

/**
 * @struct GfPollResponseArgType
 * Defines fields for the Poll Response
 */
typedef struct {
  Oid sharedMethodInvocationObj; /**< the methodInvocation object */
  int32 resultIsError;           /**< 1 indicates that the result object is an error string */
  Oid resultObj;                 /**< the result of the methodInvocation */
} GfPollResponseArgType;

/**
 * @struct GfwStringArray
 * Structure used to return an array of wide character strings.
 * Storage for the array and the strings returned is allocated in the C heap.
 * <b>
 * The user should call gfwCacheFreeStringArray to release the
 *  memory allocated for the GfwStringArray when finished processing it.
 * </b>
 */
typedef struct {
  int length;  /**< the number of elements in the array */
  wchar_t **array;  /**< an array of wide character strings */
}  GfwStringArray;

/**
 * @enum GfRegionGetState
 * Used internally to track the state of the region through a "Get" Operation
 */
typedef enum {
  GET_C_CALLED,          /**< The get is active */
  USER_C_LOADER_CALLED,  /**< The C loader function is active */
  JAVA_PROCESSOR_CALLED  /**< The JavaGet is active */
}  GfRegionGetState;

/**
 * @enum GfRegionScope
 * The region scope definitions, which must correspond
 * to the definitions in Scope.java 
 */
typedef enum {
  GF_CACHE_SCOPE_LOCAL = 0,   /**< Local */
  GF_CACHE_SCOPE_DISTRIBUTED_NO_ACK = 1, /**< DistributedNoAck */
  GF_CACHE_SCOPE_DISTRIBUTED_ACK = 2, /**< DistributedAck */
  GF_CACHE_SCOPE_GLOBAL = 3 /**< Global */
} GfRegionScope;

/**
 * A generic function definition included in the GfRegion structure to 
 * avoid circular type references.  Actual type of the _loaderFunction
 * is GfCacheLoaderFunc.
 */
typedef GfErrType (*genericFunc)(void *args);

/** @enum GfRegionErrorBufSize
 * the size of the buffer used to save the error text for
 * an error that occured while performing an operation on a region.
 */
typedef enum {
  GF_REGION_ERROR_BUF_SIZE = 256 
} GfRegionErrorBufSize;

/** @struct GfRegion
 * A structure used to manage the state of a region through the C cache 
 * interface. Only the first two fields are user accessable.  All fields 
 * in the remainer of this structure are private to the Gemfire C API 
 * implementation and should not be accessed directly by application code.
 * <b>
 * When finished using the region struct the user should call 
 * gfCacheReleaseRegion so that the gfRegionOid can be removed from the 
 * gfScope and any buffers allocated in the C Heap by operations on the 
 * region can be freed.
 * </b>
 */
typedef struct {
  wchar_t *name; /**< filled in by getRegion or getSubregion */
  int8    stringKeys; /**< true if keys are constrained to be String */

  Oid  _regionOid; /**< private */
  genericFunc _loaderFunction; /**< private */
  GfErrType _errCode; /**< private */
  char _errTxt[GF_REGION_ERROR_BUF_SIZE];  /**< private */
  GfRegionGetState _getState;   /**< private */
  char   *_saveValue;   /**< private */
  size_t _saveValueBufferSize;   /**< private */
  size_t _saveValueSize;   /**< private */
  char * _saveKeyConstraint; /**< private */
  char * _saveDiskFiles[4]; /**< private */
  } GfRegion;


/** @enum GfRegionMirrorType
 * The possible values for mirroring attributes for regions
 */
typedef enum {
  GF_REGION_MIRROR_NONE, /**< New entries created in other caches for this region are not automatically propagated to this region in this cache. */
  GF_REGION_MIRROR_KEYS, /**< New entries created in other caches for this region are propagated to this region in this cache, but the value is not necessarily copied to this cache with the key. */
  GF_REGION_MIRROR_KEYS_VALUES /**< New entries created in other caches for this region are propagated to this region in this cache and the value is also copied to this cache. */
} GfRegionMirrorType;


/** @struct GfRegionAttributes
 * This structure is used to set up the attributes for a new region.
 * It can be initialized with @ref gfCacheGetDefaultAttributes.  Fields
 * can then be modified and the resulting attribute structure used to 
 * create a region.
 * 
 * C Cache Listeners are not created at the time the region is created,
 * but can be added for the region at any time using @ref gfCacheSetEventListener.
 * 
 * The first three attributes are specifications for a declarable class that
 * can be loaded and executed in the Java virtual machine that is hosting the
 * CacheProxy for this process (probably the GemFire System Manager).
 * The syntax for the string is:
 *  classname [ key "value" ]*
 * in which the tokens (classname, key and "value"> must always be 
 * separated by white space.  Further, the value must ALWAYS be included 
 * in double quote characters.  The string should contain the number of 
 * key/value pairs of properties needed to initialize the class.
 *
 * For example: to associate a cacheLoader with a new region the 
 * cacheLoaderspec might be:
 *    com.company.app.DBLoader URL "jdbc://12.34.56.78/mydb"
 */

#define STRING_KEY_CONSTRAINT "java.lang.String" /**< class name for a String key constraint */

typedef struct {
  /* Callback Attributes */
  char *cacheLoaderSpec; /**< [@a default: null] a string specification for the region's cacheLoader */     char *cacheWriterSpec; /**< a string specification for the region's cacheWriter */
  char *capacityControllerSpec;
  /**< a string specification for the region's capacityController 

   @deprecated User-defined <code>CapacityController</code>s are
   known to be unsafe and will be removed in a future release.
*/

  /* Expiration attributes all default to no expiration */
  int32 regionTimeToLive; /**< Expiration configuration for the entire region based on the lastModifiedTime. */
  int32 regionIdleTimeout; /**< Expiration configuration for the entire region based on the lastAccessedTime. */
  int32 entryTimeToLive; /**< Expiration configuration for individual entries based on the lastModifiedTime. */
  int32 entryIdleTimeout; /**< Expiration configuration for individual entries based on the lastAccessedTime. */

  /* Distribution Attributes */
  GfRegionScope scope; /**< Properties of distribution for the region, including whether it is distributed at all, whether acknowledgements are required, and whether distributed synchronization is required. */

  /* Storage */
  GfRegionMirrorType mirrorType; /**< Type of mirroring in this region, used to keep regions across caches automatically in sync with respect to new key creation and optionally the values associated with those keys. Note that subregion creation is never propagated automatically. A mirrored region also gets initialized when it is created with the superset of all the keys (and values if KEYS_VALUES) from the region with the same name in all other caches in the distributed system. Default NONE */

  char *keyConstraint; /**< The name of the Class to constrain the keys to in the region. In addition to the utility of this for applications in general, a keyConstraint  of String is currently necessary for compatibility with the the C API. Default OID_NIL, i.e. no constraint */

  int32 initialCapacity; /**< The initial capacity of the map used for storing the entries.  Default 16. */

  float loadFactor; /**< The load factor of the map used for storing the entries. Default .75 */

  int32 concurrencyLevel; /**< The allowed concurrency among updates to values in the region is guided by the concurrencyLevel, which is used as a hint for internal sizing. The actual concurrency will vary. Ideally, you should choose a value to accommodate as many threads as will ever concurrently modify values in the region. Using a significantly higher value than you need can waste space and time, and a significantly lower value can lead to thread contention. But overestimates and underestimates within an order of magnitude do not usually have much noticeable impact. A value of one is appropriate when it is known that only one thread will modify and all others will only read. Default 16. */

  int8  statisticsEnabled; /**< Whether statistics are enabled for this region. The default is disabled, which conserves on memory.  Default FALSE. */

  int8 persistBackup; /**< Whether or not a persistent backup should be made of the region.  Default false. */

  int8 earlyAck; /**< Whether or not acks are sent after the update is processed.  Default false. */

  int8 diskWriteIsSynchronous; /**< Indicates whether the disk writes are synchronous with updates.  Default false. */

  int64 diskWriteTimeInterval; /**< In asynchronous mode, the number of milliseconds that can elapse before unwritten data is written to disk. Default 0. */

  int64 diskWriteBytesThreshold; /**< In asychronous mode, The number of unwritten bytes of data that can be enqueued before being written to disk.  Default 0. */

  char * diskWriteDirs[4]; /**< The directories to which the region's data are written. 
        If multiple directories are used, GemFire will attempt to  
        distribute the data evenly among them. If none are specified then
        the current working directory(user.dir system property) is used.
     */

  } GfRegionAttributes;

/** @typedef GfCacheLoaderFunc
 * The type declaration for the user supplied loader function that is 
 * called when a cache get operation does not find a value for the key
 * in shared memory.
 * @param key        pointer to the key string
 * @param keyIsWide  indicates whether the key is ASCII or wide string format 
 * @param javaArg    an object reference that can be passed on through JavaGet
 * @param cArg       an argument that can be passed through to the C loader 
 * @param region     the region to be loaded 
 * @param valueSize  a pointer to an int32 in which the size of the value buffer is returned, must be non-null
 * @param valueBuffer a pointer to a result value buffer,  may be null
 * @param valueBufferSize specifies the size of the valueBuffer, only used if valueBuffer not NULL
 */
typedef GfErrType (*GfCacheLoaderFunc)(
  const char *key,
  int8 keyIsWide,
  Oid   javaArg,
  void* cArg,
  GfRegion *region,
  int32 *valueSize,
  int8 *valueBuffer, 
  int32 valueBufferSize);

/** 
 * @enum GfCacheEventCodes
 * The event codes used to describe the operation detected in an event */
typedef enum{
  NO_EVENT, /**< no event */
  AFTER_CREATE,  /**< After Create */
  AFTER_DESTROY,   /**< After Destroy */
  AFTER_INVALIDATE, /**< After Invalidate */
  AFTER_REGION_DESTROY,   /**< After Region Destroy */
  AFTER_REGION_INVALIDATE, /**< After Region Invalidate */
  AFTER_UPDATE, /**< After Update */
  CLOSE /**< Close Event - used to clean up local data structures */
} GfCacheEventCodes;

/** @typedef GfEventHandlerFunction
 * The type declaration for the user supplied EventHandlerFunction called
 * when an event is detected.  The implementation for this function must
 * be thread-safe because it may be called concurrently depending upon
 * the way in which events are generated. 
 * As an optimization, keys and values are transmitted to this function
 * in a buffer to avoid shared memory object creation.  These buffers
 * are allocated and managed by the C interface and may only be referenced 
 * during the execution of the event handler function.
 * DO NOT PASS THE BUFFER POINTERS TO OTHER THREADS.
 *
 * @param  eventCode   the code for the event 
 * @param  boolValues  bits that describe event characteristics, e.g. GF_CACHE_EVENT_ORIGIN_REMOTE
 * @param  region      the region that the event occurred in 
 * @param  callbackArg a callback arg that can be used to process the event
 * @param  key         the key for an entry in the region, OID_NIL if keyBuf is used 
 * @param  newValue    the new value for the region entry, OID_NIL if valueBuf is used 
 * @param  oldValue    the old (previous) value of the region entry, OID_NIL if oldValueBuf is used
 * @param  keyLen      the length of the key (in characters) if it is a string
 * @param  keyBuf      the value of the key
 * @param  newValueSize the number of bytes in the newValue buffer
 * @param  newValueBuf  the bytes of the newValue 
 * @param  oldValueSize the number of bytes in the oldValue buffer 
 * @param  oldValueBuf  the bytes of the oldValue 
 */
typedef GfErrType (*GfEventHandlerFunction)(
  GfCacheEventCodes  eventCode, /**< the event code for the event */
  int8     boolValues,
  Oid      region, 
  Oid      callbackArg,
  Oid      key, 
  Oid      newValue, 
  Oid      oldValue, 
  int32    keyLen, 
  wchar_t  *keyBuf,
  int32    newValueSize, 
  int8     *newValueBuf, 
  int32    oldValueSize, 
  int8     *oldValueBuf 
);

/** Definitions of bit fields in an int32 used to pass information about
 *  an event to the event handler
 */
#define GF_CACHE_EVENT
/** bit field mask, set if the event was caused by a local load */
#define GF_CACHE_EVENT_LOCAL_LOAD     0x01
/** bit field mask, set if the event was caused by a net load */
#define GF_CACHE_EVENT_NET_LOAD       0x02
/** bit field mask, set if the event was caused by a net search */
#define GF_CACHE_EVENT_NETSEARCH      0x04
/** bit field mask, set if the event was caused by a dstributed operation */
#define GF_CACHE_EVENT_DISTRIBUTED    0x08
/** bit field mask, set if the event was caused by an expiration */
#define GF_CACHE_EVENT_EXPIRATION     0x10
/** bit field mask, set if the event was originated in a cache other than this one */
#define GF_CACHE_EVENT_ORIGIN_REMOTE  0x20

/** Definitions for event masks which can be used to filter certain events
 *  so that the handler will not get them.  Masks can be arithimeticly or'd
 *  together.
 */
#define GF_CACHE_EVENT_MASKS
/** mask to use to get AfterCreate events */
#define GF_CACHE_EVENT_MASK_AFTER_CREATE            0x01
/** mask to use to get AfterDestroy events */
#define GF_CACHE_EVENT_MASK_AFTER_DESTROY           0x02
/** mask to use to get AfterInvalidate events */
#define GF_CACHE_EVENT_MASK_AFTER_INVALIDATE        0x04
/** mask to use to get AfterRegionDestroy events */
#define GF_CACHE_EVENT_MASK_AFTER_REGION_DESTROY    0x08
/** mask to use to get AfterRegionInvalidate events */
#define GF_CACHE_EVENT_MASK_AFTER_REGION_INVALIDATE 0x10
/** mask to use to get AfterUpdate events */
#define GF_CACHE_EVENT_MASK_AFTER_UPDATE            0x20
/** mask to use to get AfterClose events */
#define GF_CACHE_EVENT_MASK_CLOSE                   0x40
/** mask to use to get All events */
#define GF_CACHE_EVENT_MASK_ALL                     0x7F

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @defgroup A Connection Management
 * @{
 */
GFCEXTERN(GfErrType) gfConnect(const char *sysdir, const char *connectionName, int8 writeProtectAllowed);
GFCEXTERN(GfErrType) gfDisconnect(void);
GFCEXTERN(int32)     gfConnectionStatus(void);
GFCEXTERN(GfErrType) gfGetSystemInfo(GfSysInfo *info);
GFCEXTERN(GfErrType) gfWriteProtect(int32 protect);
/**
 * @}
 */


/**
 * @defgroup B Namespace Management
 * @{
 */
GFCEXTERN(GfErrType) gfContainsKey(const char *name, int8 *result);
GFCEXTERN(GfErrType) gfContainsKeyW(const wchar_t *name, int8 *result);
GFCEXTERN(GfErrType) gfGet(const char *name, Oid *result);
GFCEXTERN(GfErrType) gfGetW(const wchar_t *name, Oid *result);
GFCEXTERN(GfErrType) gfPut(const char *name, Oid object, Oid *oldValue);
GFCEXTERN(GfErrType) gfPutW(const wchar_t *name, Oid object, Oid *oldValue);
GFCEXTERN(GfErrType) gfRemove(const char *name, Oid *value);
GFCEXTERN(GfErrType) gfRemoveW(const wchar_t *name, Oid *value);
GFCEXTERN(GfErrType) gfGetKeys(Oid *buffer, int32 maxKeys, int32 *keysReturned);
/**
 * @}
 */

/**
 * @defgroup C Reference Management
 * @{
 */
GFCEXTERN(GfErrType) gfAddToScope(Oid object);
GFCEXTERN(GfErrType) gfEmptyGlobalScope(void);
GFCEXTERN(GfErrType) gfEnterScope(void);
GFCEXTERN(GfErrType) gfExitScope(void);
GFCEXTERN(GfErrType) gfReturnScope(void);
GFCEXTERN(GfErrType) gfReturnPartialScope(Oid retainList[], int32 numOids);
GFCEXTERN(GfErrType) gfGetScopeDepth(int32 *result);
GFCEXTERN(GfErrType) gfRelease(Oid object);
GFCEXTERN(GfErrType) gfReleaseList(Oid objects[], int32 numOids);
/**
 * @}
 */

/**
 * @defgroup D Error Handling, Miscellaneous
 * @{
 */
GFCEXTERN(void) gfErr(GfErrorInfo *errorStruct);
GFCEXTERN(int64) gfCurrentTimeNanos(void);

/**
 * @}
 */

/**
 * @defgroup E Object Semaphores
 * @{
 */
GFCEXTERN(GfErrType) gfSemaSignal(Oid sharedObject);
GFCEXTERN(GfErrType) gfSemaWait(Oid sharedObject);
GFCEXTERN(GfErrType) gfSemaTimedWait(Oid sharedObject, int64 waitMs);
GFCEXTERN(GfErrType) gfSemaGetWaitCount(Oid sharedObject, int32 *result);
GFCEXTERN(GfErrType) gfSemaGetSignalCount(Oid sharedObject, int64 *result);
GFCEXTERN(GfErrType) gfCreateSharedSemaphore(int64 initialCount, Oid *result);
/**
 * @}
 */

/**
 * @defgroup F SharedLinkedList
 * @{
 */
GFCEXTERN(GfErrType) gfSLLAdd(Oid sharedList, Oid element, Oid insertBeforeSLN, Oid *nodeReturn);
GFCEXTERN(GfErrType) gfSLLAddFirst(Oid sharedList, Oid element, Oid *nodeReturn);
GFCEXTERN(GfErrType) gfSLLAddLast(Oid sharedList, Oid element, Oid *nodeReturn);
GFCEXTERN(GfErrType) gfSLLClear(Oid sharedList);
GFCEXTERN(GfErrType) gfSLLCreate(Oid *result);
GFCEXTERN(GfErrType) gfSLLHead(Oid sharedList, Oid *node);
GFCEXTERN(GfErrType) gfSLLTail(Oid sharedList, Oid *node);
GFCEXTERN(GfErrType) gfSLLIsEmpty(Oid sharedList, int8 *result);
GFCEXTERN(GfErrType) gfSLLRemove(Oid sharedList, Oid node, Oid *element);
GFCEXTERN(GfErrType) gfSLLRemoveHead(Oid sharedList, Oid *element);
GFCEXTERN(GfErrType) gfSLLRemoveTail(Oid sharedList, Oid *element);
GFCEXTERN(GfErrType) gfSLLSize(Oid sharedList, int32 *result);

GFCEXTERN(GfErrType) gfSLNNext(Oid sharedListNode, Oid *result);
GFCEXTERN(GfErrType) gfSLNPrev(Oid sharedListNode, Oid *result);
GFCEXTERN(GfErrType) gfSLNElement(Oid sharedListNode, Oid *result);
GFCEXTERN(GfErrType) gfSLNOwner(Oid sharedListNode, Oid *result);
/**
 * @}
 */

/**
 * @defgroup G SharedIntMap
 * @{
 */
GFCEXTERN(GfErrType) gfSIMCreate(int32 initialSize, Oid *result);
GFCEXTERN(GfErrType) gfSIMContainsKey(Oid intMap, int32 key, int8 *result);
GFCEXTERN(GfErrType) gfSIMFetchKeys(Oid intMap, int32 *keyBuffer, 
                                    int32 maxKeys, int32 *keysReturned);
GFCEXTERN(GfErrType) gfSIMFetchValues(Oid intMap, Oid *valueBuffer, 
                                      int32 maxValues, int32 *numReturned);
GFCEXTERN(GfErrType) gfSIMGet(Oid intMap, int32 key, Oid *result);
GFCEXTERN(GfErrType) gfSIMIsEmpty(Oid intMap, int8 *result);
GFCEXTERN(GfErrType) gfSIMPut(Oid intMap, int32 key, Oid value, Oid *oldValue);
GFCEXTERN(GfErrType) gfSIMRemoveAll(Oid intMap);
GFCEXTERN(GfErrType) gfSIMRemove(Oid intMap, int32 key, Oid *oldValue);
GFCEXTERN(GfErrType) gfSIMRehash(Oid intMap, int32 newTableSize);
GFCEXTERN(GfErrType) gfSIMSize(Oid intMap, int32 *result);
/**
 * @}
 */

/**
 * @defgroup H SharedStringMap
 * @{
 */
GFCEXTERN(GfErrType) gfSSMCreate(int32 initialSize, Oid *result);
GFCEXTERN(GfErrType) gfSSMFetchKeys(Oid stringMap, Oid* keyBuffer, int32 maxKeys, int32 *keysReturned);
GFCEXTERN(GfErrType) gfSSMFetchValues(Oid stringMap, Oid *valueBuffer, int32 maxValues, int32 *numReturned);
GFCEXTERN(GfErrType) gfSSMIsEmpty(Oid stringMap, int8 *result);
GFCEXTERN(GfErrType) gfSSMContainsKey(Oid stringMap, const char *key, int8 *result);
GFCEXTERN(GfErrType) gfSSMContainsKeyW(Oid stringMap, const wchar_t *key, int8 *result);
GFCEXTERN(GfErrType) gfSSMGet(Oid stringMap, const char *key, Oid *result);
GFCEXTERN(GfErrType) gfSSMGetW(Oid stringMap, const wchar_t *key, Oid *result);
GFCEXTERN(GfErrType) gfSSMPut(Oid stringMap, const char *key, Oid value, Oid *oldValue);
GFCEXTERN(GfErrType) gfSSMPutW(Oid stringMap, const wchar_t *key, Oid value, Oid *oldValue);
GFCEXTERN(GfErrType) gfSSMRehash(Oid stringMap, int32 newTableSize);
GFCEXTERN(GfErrType) gfSSMRemoveAll(Oid stringMap);
GFCEXTERN(GfErrType) gfSSMRemove(Oid stringMap, const char *key, Oid *oldValue);
GFCEXTERN(GfErrType) gfSSMRemoveW(Oid stringMap, const wchar_t *key, Oid *oldValue);
GFCEXTERN(GfErrType) gfSSMSize(Oid stringMap, int32 *result);
/**
 * @}
 */  
     
/**  
 * @defgroup I SharedLinkedBlockingQueue
 * @{
 */  
GFCEXTERN(GfErrType) gfSLQCreate(Oid *result);
GFCEXTERN(GfErrType) gfSLQCreateCapacity(int32 capacity, Oid *result); 
GFCEXTERN(GfErrType) gfSLQIsEmpty(Oid sharedQueue, int8 *result);
GFCEXTERN(GfErrType) gfSLQPeek(Oid sharedQueue, Oid *result);
GFCEXTERN(GfErrType) gfSLQTake(Oid sharedQueue, Oid *result);
GFCEXTERN(GfErrType) gfSLQPoll(Oid sharedQueue, int64 timeoutMs, Oid *result);
GFCEXTERN(GfErrType) gfSLQPut(Oid sharedQueue, Oid element);  
GFCEXTERN(GfErrType) gfSLQOffer(Oid sharedQueue, Oid element, int64 timeoutMs); 
GFCEXTERN(GfErrType) gfSLQSize(Oid sharedQueue, int32 *result);
GFCEXTERN(GfErrType) gfSLQToArray(Oid sharedQueue, int32 maxReturn,
                                  Oid *elementArray, int32 *numReturned);
GFCEXTERN(GfErrType) gfSLQRemainingCapacity(Oid sharedQueue, int32 *result);
/**
 * @}
 */

/**
 * @defgroup J Shared Classes & Fields
 * @{
 */
GFCEXTERN(GfErrType) gfCreateSharedClass(const char *className, const char *description, Oid fields[],
                    int32 numFields, int32 isCopyShared, Oid *result);
GFCEXTERN(GfErrType) gfCreateSharedClassW(const char *className, const wchar_t *description, Oid fields[],
                    int32 numFields, int32 isCopyShared, Oid *result);
GFCEXTERN(GfErrType) gfGetClass(Oid object, Oid *result);
GFCEXTERN(GfErrType) gfGetClassName(Oid sharedClass, char *className,
                         int32 maxLength, int32 *lengthReturned);
GFCEXTERN(GfErrType) gfGetClassNameW(Oid sharedClass, wchar_t *className,
                          int32 maxLength, int32 *lengthReturned);
GFCEXTERN(GfErrType) gfGetClassDescription(Oid sharedClass, char *classDesc,    
                                int32 maxLength, int32 *lengthReturned);
GFCEXTERN(GfErrType) gfGetClassDescriptionW(Oid sharedClass, wchar_t *classDesc,
                                 int32 maxLength, int32 *lengthReturned);
GFCEXTERN(GfErrType) gfGetNumFields(Oid sharedClass, int32 *result);
GFCEXTERN(GfErrType) gfGetClassFields(Oid sharedClass, Oid fields[],
                           int32 maxFields, int32 *numReturned);
GFCEXTERN(GfErrType) gfGetClassIsCopyShared(Oid sharedClass, int8 *result);

GFCEXTERN(GfErrType) gfCreateField(const char *fieldName, 
                        const char *description, const char *unit,
                        int8 isCounter, GfFieldType type, Oid *result);
GFCEXTERN(GfErrType) gfCreateFieldW(const char *fieldName, 
                        const wchar_t * description, const wchar_t *unit,
                        int8 isCounter, int32 type, Oid *result);  
GFCEXTERN(GfErrType) gfGetFieldName(Oid field, char *fieldName,
                         int32 maxLength, int32 *lengthReturned);  
GFCEXTERN(GfErrType) gfGetFieldNameW(Oid field, wchar_t *fieldName,
                          int32 maxLength, int32 *lengthReturned); 
GFCEXTERN(GfErrType) gfGetFieldDescription(Oid field, char *fieldDesc,
                         int32 maxLength, int32 *lengthReturned);
GFCEXTERN(GfErrType) gfGetFieldDescriptionW(Oid field, wchar_t *fieldDesc,   
                          int32 maxLength, int32 *lengthReturned);
GFCEXTERN(GfErrType) gfGetFieldUnit(Oid field, char *fieldUnit,
                         int32 maxLength, int32 *lengthReturned);
GFCEXTERN(GfErrType) gfGetFieldUnitW(Oid field, wchar_t *fieldUnit,
                          int32 maxLength, int32 *lengthReturned);
GFCEXTERN(GfErrType) gfGetFieldIsCounter(Oid field, int8 *result);
GFCEXTERN(GfErrType) gfGetFieldType(Oid field, int32 *result);
GFCEXTERN(GfErrType) gfGetFieldOffset(Oid field, int32 *result);
/**
 * @}
 */  
     
/**  
 * @defgroup K Object Creation & Access
 * @{
 */  
GFCEXTERN(GfErrType) gfCreate(Oid sharedClass, Oid *result);
GFCEXTERN(GfErrType) gfKeepInMemory(Oid object, int8 newState);
GFCEXTERN(GfErrType) gfFetchOid(Oid sharedObject, int32 offset, Oid *result);
GFCEXTERN(GfErrType) gfStoreOid(Oid sharedObject, int32 offset, Oid value);
GFCEXTERN(GfErrType) gfFetchBoolean(Oid sharedObject, int32 offset, int8 *result);
GFCEXTERN(GfErrType) gfStoreBoolean(Oid sharedObject, int32 offset, int8 value);
GFCEXTERN(GfErrType) gfFetchChar(Oid sharedObject, int32 offset, char *result);
GFCEXTERN(GfErrType) gfStoreChar(Oid sharedObject, int32 offset, char value);
GFCEXTERN(GfErrType) gfFetchWChar(Oid sharedObject, int32 offset, wchar_t *result);
GFCEXTERN(GfErrType) gfStoreWChar(Oid sharedObject, int32 offset, wchar_t value);
GFCEXTERN(GfErrType) gfFetchByte(Oid sharedObject, int32 offset, int8 *result);
GFCEXTERN(GfErrType) gfStoreByte(Oid sharedObject, int32 offset, int8 value);
GFCEXTERN(GfErrType) gfIncByte(Oid sharedObject, int32 fieldOffset, int8 amount, int8 *result);
GFCEXTERN(GfErrType) gfFetchShort(Oid sharedObject, int32 offset, int16 *result);
GFCEXTERN(GfErrType) gfStoreShort(Oid sharedObject, int32 offset, int16 value);
GFCEXTERN(GfErrType) gfIncShort(Oid sharedObject, int32 fieldOffset, int16 amount, int16 *result);
GFCEXTERN(GfErrType) gfFetchInt(Oid sharedObject, int32 offset, int32 *result);
GFCEXTERN(GfErrType) gfStoreInt(Oid sharedObject, int32 offset, int32 value);
GFCEXTERN(GfErrType) gfIncInt(Oid sharedObject, int32 fieldOffset, int32 amount, int32 *result);
GFCEXTERN(GfErrType) gfAndInt(Oid sharedObject, int32 offset, int32 value);
GFCEXTERN(GfErrType) gfOrInt(Oid sharedObject, int32 offset, int32 value);
GFCEXTERN(GfErrType) gfFetchLong(Oid sharedObject, int32 offset, int64 *result);
GFCEXTERN(GfErrType) gfStoreLong(Oid sharedObject, int32 offset, int64 value);
GFCEXTERN(GfErrType) gfIncLong(Oid sharedObject, int32 fieldOffset, int64 amount, int64 *result);
GFCEXTERN(GfErrType) gfFetchFloat(Oid sharedObject, int32 offset, float *result);
GFCEXTERN(GfErrType) gfStoreFloat(Oid sharedObject, int32 offset, float value);
GFCEXTERN(GfErrType) gfIncFloat(Oid sharedObject, int32 fieldOffset, float amount, float *result);
GFCEXTERN(GfErrType) gfFetchDouble(Oid sharedObject, int32 offset, double *result);
GFCEXTERN(GfErrType) gfStoreDouble(Oid sharedObject, int32 offset, double value);
GFCEXTERN(GfErrType) gfIncDouble(Oid sharedObject, int32 fieldOffset, double amount, double *result);
GFCEXTERN(GfErrType) gfStoreString(Oid sharedObject, int32 offset, const char *value);
GFCEXTERN(GfErrType) gfStoreWString(Oid sharedObject, int32 offset, const wchar_t *value);
GFCEXTERN(GfErrType) gfGetStatsArchived(Oid sharedObject, int8 *result);
GFCEXTERN(GfErrType) gfSetStatsArchived(Oid sharedObject, int8 newValue);
GFCEXTERN(GfErrType) gfArchiveStats(Oid sharedObject, const char* name, int64 statId);
/**
 * @}
 */

/**
 * @defgroup L Arrays
 * @{
 */
GFCEXTERN(GfErrType) gfCreateArray(Oid sharedClass, int32 length, Oid *result);
GFCEXTERN(GfErrType) gfCreateCsObjArray(int32 length, Oid elementType, Oid *result);
GFCEXTERN(GfErrType) gfFetchArray(void *dest, int32 destSize, Oid srcObject, 
                                  int32 srcIdx, int32 numElements, int32 *numReturned);
GFCEXTERN(GfErrType) gfFetchWCharArray(wchar_t *dest, int32 destSize, Oid sharedArray, int32 srcIdx, int32 numElements, int32 *numReturned);
GFCEXTERN(GfErrType) gfGetArrayLength(Oid sharedObj, int32 *result);
GFCEXTERN(GfErrType) gfGetCsObjArrayType(Oid sharedObj, Oid *result);
GFCEXTERN(GfErrType) gfIsArray(Oid sharedObj, int8 *result);
GFCEXTERN(GfErrType) gfMemcpy(Oid dest, int32 destIdx, Oid src, int32 
                              srcIdx, int32 length, int32 *numCopied);
GFCEXTERN(GfErrType) gfStoreArray(Oid destObject, int32 destIdx, void *src, 
                                  int32 numElements, int32 *numStored);
GFCEXTERN(GfErrType) gfStoreWCharArray(Oid destObject, int32 destIdx, 
		      wchar_t *src, int32 numElements, int32 *numStored);
/**
 * @}
 */

/**
 * @defgroup M Strings
 * @{
 */
GFCEXTERN(GfErrType) gfCharToWchar(const char *srcPtr, wchar_t *destPtr, int32 lentth);
GFCEXTERN(GfErrType) gfWcharToChar(const wchar_t *srcPtr, char *destPtr, int32 length);
GFCEXTERN(GfErrType) gfWcharStrLen(const wchar_t *srcPtr, int32 *length);
GFCEXTERN(GfErrType) gfCreateStr (const char *source, Oid *result);
GFCEXTERN(GfErrType) gfCreateWStr(const wchar_t *source, Oid *result);
GFCEXTERN(GfErrType) gfGetStrLength(Oid sharedString, int32 *result);
GFCEXTERN(GfErrType) gfIsWideStr(Oid sharedString, int8 *result);
GFCEXTERN(GfErrType) gfFetchStr(Oid sharedString, int32 srcIdx, char *dest, int32 maxLength, int32 *numFetched);
GFCEXTERN(GfErrType) gfFetchWStr(Oid sharedString, int32 srcIdx, wchar_t *dest, int32 maxLength, int32 *numFetched);
/**
 * @}
 */

/**
 * @defgroup N Wrappers
 * @{
 */
GFCEXTERN(GfErrType) gfwCreateBoolean(int8 value, Oid *result);
GFCEXTERN(GfErrType) gfwFetchBoolean(Oid wrapper, int8 *result);
GFCEXTERN(GfErrType) gfwCreateByte(int8 value, Oid *result);
GFCEXTERN(GfErrType) gfwFetchByte(Oid wrapper, int8 *result);
GFCEXTERN(GfErrType) gfwCreateChar(char value, Oid *result);
GFCEXTERN(GfErrType) gfwFetchChar(Oid wrapper, char *result);
GFCEXTERN(GfErrType) gfwCreateWChar(wchar_t value, Oid *result);
GFCEXTERN(GfErrType) gfwFetchWChar(Oid wrapper, wchar_t *result);
GFCEXTERN(GfErrType) gfwCreateShort(int16 value, Oid *result);
GFCEXTERN(GfErrType) gfwFetchShort(Oid wrapper, int16 *result);
GFCEXTERN(GfErrType) gfwCreateInt(int32 value, Oid *result);
GFCEXTERN(GfErrType) gfwFetchInt(Oid wrapper, int32 *result);
GFCEXTERN(GfErrType) gfwCreateLong(int64 value, Oid *result);
GFCEXTERN(GfErrType) gfwFetchLong(Oid wrapper, int64 *result);
GFCEXTERN(GfErrType) gfwCreateFloat(float value, Oid *result);
GFCEXTERN(GfErrType) gfwFetchFloat(Oid wrapper, float *result);
GFCEXTERN(GfErrType) gfwCreateDouble(double value, Oid *result);
GFCEXTERN(GfErrType) gfwFetchDouble(Oid wrapper, double *result);
GFCEXTERN(GfErrType) gfwCreateByteArray(int32 arrayLength, int8 *arrayBufPtr, Oid *result);
GFCEXTERN(GfErrType) gfwGetBytes(Oid wrapper, int32 offset, int32 maxReturn, char* bufferPtr, int32* numBytesReturned);
GFCEXTERN(GfErrType) gfwGetArrayLength(Oid wrapper, int32 offset, int32* arrayLength);
GFCEXTERN(GfErrType) gfwPutBytes(Oid wrapper, int32 offset, int32 arrayLength, char* bufferPtr);
GFCEXTERN(GfErrType) gfwReplaceRecycle(Oid wrapper, int32 offset, Oid newValue);
GFCEXTERN(GfErrType) gfwIdentityEquals(Oid wrapper, int32 offset, Oid newValue, int8 *result);
/**
 * @}
 */

/**
 * @defgroup O Logging
 * @{
 */
GFCEXTERN(GfErrType) gfLogFinestEnabled(int8 *result);
GFCEXTERN(GfErrType) gfLogFinerEnabled(int8 *result);
GFCEXTERN(GfErrType) gfLogFineEnabled(int8 *result);
GFCEXTERN(GfErrType) gfLogConfigEnabled(int8 *result);
GFCEXTERN(GfErrType) gfLogInfoEnabled(int8 *result);
GFCEXTERN(GfErrType) gfLogWarningEnabled(int8 *result);
GFCEXTERN(GfErrType) gfLogSevereEnabled(int8 *result);

GFCEXTERN(GfErrType) gfLogFinest(const char *msg);
GFCEXTERN(GfErrType) gfLogFiner(const char *msg);
GFCEXTERN(GfErrType) gfLogFine(const char *msg);
GFCEXTERN(GfErrType) gfLogConfig(const char *msg);
GFCEXTERN(GfErrType) gfLogInfo(const char *msg);
GFCEXTERN(GfErrType) gfLogWarning(const char *msg);
GFCEXTERN(GfErrType) gfLogSevere(const char *msg);

GFCEXTERN(GfErrType) gfLogEntering(const char *module, const char *function);
GFCEXTERN(GfErrType) gfLogExiting(const char *module, const char *function);

GFCEXTERN(GfErrType) gfLogFinestW(const wchar_t *msg);
GFCEXTERN(GfErrType) gfLogFinerW(const wchar_t *msg);
GFCEXTERN(GfErrType) gfLogFineW(const wchar_t *msg);
GFCEXTERN(GfErrType) gfLogConfigW(const wchar_t *msg);
GFCEXTERN(GfErrType) gfLogInfoW(const wchar_t *msg);
GFCEXTERN(GfErrType) gfLogWarningW(const wchar_t *msg);
GFCEXTERN(GfErrType) gfLogSevereW(const wchar_t *msg);

GFCEXTERN(GfErrType) gfLogEnteringW(const wchar_t *module, const wchar_t *function);
GFCEXTERN(GfErrType) gfLogExitingW(const wchar_t *module, const wchar_t *function);
/**
 * @}
 */  
     
/**
 * @defgroup Q Locking, Waiting and Notifying
 * @{
 */  
GFCEXTERN(GfErrType) gfAcquireLock(Oid sharedObject);
GFCEXTERN(GfErrType) gfIsAcquired(Oid sharedObject, int8 *result);
GFCEXTERN(GfErrType) gfNotify(Oid sharedObject);
GFCEXTERN(GfErrType) gfNotifyAll(Oid sharedObject);
GFCEXTERN(GfErrType) gfReleaseLock(Oid sharedObj);
GFCEXTERN(GfErrType) gfWait(Oid sharedObject, int64 waitMs);
/**
 * @}
 */  
     
     
/**
 * @defgroup R Shared Memory Listeners
 * @{
 */  

GFCEXTERN(GfErrType) gfCreateListenerList(Oid *result);

GFCEXTERN(GfErrType) gfCreateListener(Oid listenerList, const char *listenerDescription,
                                        Oid *result);
                                        
GFCEXTERN(GfErrType) gfGetListenersLength(Oid listenerList, int32 *result);

GFCEXTERN(GfErrType) gfFetchListener(Oid listenerList, int32 offset, Oid *result);

GFCEXTERN(GfErrType) gfRemoveListener(Oid listenerList, Oid listenerAgent);

GFCEXTERN(GfErrType) gfListen(Oid listenerAgent, GfListenArgType *listenBuffer);

GFCEXTERN(GfErrType) gfTimedListen(Oid listenerAgent, int64 timeoutMillis,
                                   GfListenArgType *listenBuffer);
                                   
GFCEXTERN(GfErrType) gfNotifyListener(Oid listenerAgent, GfNotifyListenerArgType *notifyArg,
  int64 timeoutMillis, GfPollResponseArgType *response);
  
GFCEXTERN(GfErrType) gfNotifyAllListeners(Oid listenerList, GfNotifyListenerArgType *notifyArg,
        Oid *methodInvocationsArray);
        
GFCEXTERN(GfErrType) gfPollResponse(GfPollResponseArgType *message,
                int8 *responseReceived);
                
GFCEXTERN(GfErrType) gfSendReply(GfListenArgType *message, Oid result);

GFCEXTERN(GfErrType) gfSendError(GfListenArgType *message, Oid result);

GFCEXTERN(GfErrType) gfInitSharedMethodInvocation(GfNotifyListenerArgType* msg,
                         const char* methodName, Oid useMethodInvocationObj);


GFCEXTERN(GfFieldType) gfGetMethodArgType(Oid arg);
                                   
/**
 * @}
 */


/**
 * @defgroup S Cache Operations
 * @{
 */  
GFCEXTERN(GfErrType) gfwCacheGetRootRegions(GfwStringArray *result);
GFCEXTERN(GfErrType) gfCacheGetRegion(const char *pathname, GfRegion* region, 
                        GfCacheLoaderFunc loader);
GFCEXTERN(GfErrType) gfwCacheGetRegion(const wchar_t *pathname, 
                        GfRegion* region, GfCacheLoaderFunc loader);
GFCEXTERN(GfErrType) gfwCacheKeys(GfRegion* region, GfwStringArray *result);
GFCEXTERN(GfErrType) gfCacheContainsKey(GfRegion* region, 
                        const char* key, int8 *result);
GFCEXTERN(GfErrType) gfwCacheContainsKey(GfRegion* region, 
                        const wchar_t* key, int8 *result);
GFCEXTERN(GfErrType) gfCacheContainsValueForKey(GfRegion* region, 
                        const char* key, int8 *result);
GFCEXTERN(GfErrType) gfwCacheContainsValueForKey(GfRegion* region, 
                        const wchar_t* key, int8 *result);
GFCEXTERN(GfErrType) gfCacheInvalidateRegion(GfRegion* region, 
                        int8 distribute, Oid callbackArg);
GFCEXTERN(GfErrType) gfCacheLockRegion(GfRegion* region, 
                        int64 waitTimeMillis, int8 *result);
GFCEXTERN(GfErrType) gfCacheUnlockRegion(GfRegion* region);
GFCEXTERN(GfErrType) gfwCacheGetFullPath(GfRegion* region, 
                        GfwStringArray *result);
GFCEXTERN(GfErrType) gfCacheGetParentRegion(GfRegion* region, 
                        GfRegion* parent, GfCacheLoaderFunc loader);
GFCEXTERN(GfErrType) gfCacheGetSubregion(GfRegion* region, 
                        const char* name, GfRegion* subregion, 
                        GfCacheLoaderFunc loader);
GFCEXTERN(GfErrType) gfwCacheGetSubregion(GfRegion* region, 
                        const wchar_t* name, GfRegion* subregion, 
                        GfCacheLoaderFunc loader);
GFCEXTERN(GfErrType) gfwCacheSubregionPaths(GfRegion* region, 
                        int8 recursive, GfwStringArray *result);
GFCEXTERN(GfErrType) gfwCacheFreeStringArray(GfwStringArray *a);
GFCEXTERN(GfErrType) gfCacheSetError(GfRegion* region, GfErrType code, 
                        const char* errorText);
GFCEXTERN(GfErrType) gfCacheGetError(GfRegion* region, 
                        int errTxtBufSize , char *errTxtBuf);
GFCEXTERN(GfErrType) gfCacheReleaseRegion(GfRegion *region);
GFCEXTERN(GfErrType) gfCacheSetCommBufferSize(int32 bufferSize);
GFCEXTERN(GfErrType) gfCacheGet(GfRegion* region, const char *key, 
                        Oid callbackArg, void *cCallbackArg, 
                        int32 *valueSize,  int8 *valueBuffer, /* may be null*/
                        int32 valueBufferSize  /* only used if valueBuffer not NULL*/);
GFCEXTERN(GfErrType) gfwCacheGet(GfRegion* region, const wchar_t *key, 
                        Oid callbackArg, void *cCallbackArg, 
                        int32 *valueSize,  int8 *valueBuffer, /* may be null*/
                        int32 valueBufferSize  /* only used if valueBuffer not NULL*/);
GFCEXTERN(GfErrType) gfCacheGetObject(GfRegion* region, const char *key, 
                        Oid callbackArg, Oid *result);
GFCEXTERN(GfErrType) gfwCacheGetObject(GfRegion* region, const wchar_t *key, 
                        Oid callbackArg, Oid *result);
GFCEXTERN(GfErrType) gfCachePut(GfRegion* region, const char *key, int8 *value, 
                        int32 valueSize, Oid callbackArg);
GFCEXTERN(GfErrType) gfwCachePut(GfRegion* region, const wchar_t *key, int8 *value, 
                        int32 valueSize, Oid callbackArg);
GFCEXTERN(GfErrType) gfCachePutObject(GfRegion* region, const char *key,
                        Oid value, Oid callbackArg);
GFCEXTERN(GfErrType) gfwCachePutObject(GfRegion* region, const wchar_t *key,
                        Oid value, Oid callbackArg);
GFCEXTERN(GfErrType) gfCacheSaveValue(GfRegion* region, int8 *value, int32 valueSize);
GFCEXTERN(GfErrType) gfCacheFetchValue(GfRegion* region, int32 startingAt, 
                        int8 *buffer, int32 bufferSize, int32 *bytesReturned);
GFCEXTERN(GfErrType) gfCacheJavaGet(GfRegion* region, const char *key, 
                        Oid callbackArg);
GFCEXTERN(GfErrType) gfwCacheJavaGet(GfRegion* region, const wchar_t *key, 
                        Oid callbackArg);
GFCEXTERN(GfErrType) gfCacheCreate(GfRegion* region, const char *key, 
                        int8 *value, int32 valueSize, Oid callbackArg);
GFCEXTERN(GfErrType) gfwCacheCreate(GfRegion* region, const wchar_t* key, 
                        int8 *value, int32 valueSize, Oid callbackArg);
GFCEXTERN(GfErrType) gfCacheInvalidate(GfRegion* region, const char *key, 
                        int8 distribute, Oid callbackArg);
GFCEXTERN(GfErrType) gfwCacheInvalidate(GfRegion* region, const wchar_t *name, 
                        int8 distribute, Oid callbackArg);
GFCEXTERN(GfErrType) gfCacheDestroy(GfRegion* region, const char *key, 
                        int8 distribute, Oid callbackArg);
GFCEXTERN(GfErrType) gfwCacheDestroy(GfRegion* region, const wchar_t *key, 
                        int8 distribute, Oid callbackArg);
GFCEXTERN(GfErrType) gfCacheLock(GfRegion* region, const char *key, 
                        int64 waitTimeMillis, int8 *result);
GFCEXTERN(GfErrType) gfwCacheLock(GfRegion* region, const wchar_t *key, 
                        int64 waitTimeMillis, int8 *result);
GFCEXTERN(GfErrType) gfCacheUnlock(GfRegion* region, const char *key);
GFCEXTERN(GfErrType) gfwCacheUnlock(GfRegion* region, const wchar_t *key);
GFCEXTERN(GfErrType) gfCacheSetEventListener(GfRegion* region, 
                        GfEventHandlerFunction eventHandler, int8 eventMask);
GFCEXTERN(GfErrType) gfCacheClearEventListener(GfRegion* region);
GFCEXTERN(GfErrType) gfInitRegion(Oid regionOid, GfRegion* region);

GFCEXTERN(GfErrType) gfCacheCreateRegion(const char *name, 
                                  GfRegionAttributes *attributes,
                                  GfRegion *region,
                                  GfCacheLoaderFunc loader);
GFCEXTERN(GfErrType) gfwCacheCreateRegion(const wchar_t *name, 
                                  GfRegionAttributes *attributes,
                                  GfRegion *region,
                                  GfCacheLoaderFunc loader);
GFCEXTERN(GfErrType) gfCacheCreateSubregion(GfRegion *region,
                                  const char *name,
                                  GfRegionAttributes *attributes,
                                  GfRegion *subregion,
                                  GfCacheLoaderFunc loader);
GFCEXTERN(GfErrType) gfwCacheCreateSubregion(GfRegion *region,
                                  const wchar_t *name, 
                                  GfRegionAttributes *attributes,
                                  GfRegion *subregion,
                                  GfCacheLoaderFunc loader);
GFCEXTERN(GfErrType) gfCacheGetDefaultAttributes(GfRegionAttributes *attributes);
GFCEXTERN(GfErrType) gfCacheGetRegionAttributes(GfRegion *region, GfRegionAttributes *attributes);
GFCEXTERN(GfErrType) gfCacheGetNamedAttributes(const char* id, GfRegionAttributes *attributes);

GFCEXTERN(GfErrType) gfCacheDestroyRegion(GfRegion *region,  int8 distribute, Oid callbackArg);
/**                     
 * @}
 */


/* ------- New functions must be added to SolarisMapFile_global.mak -------*/

#ifdef __cplusplus
}
#endif

#endif /* GEMFIRE_H */
