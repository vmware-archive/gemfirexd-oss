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
#define SM_HELPER_CPP TRUE
#include "flag.ht"
#define index work_around_gcc_bug
#include "jni.h"
#undef index

#if defined(FLG_UNIX)
#include <sys/mman.h>
#include <sys/stat.h> /* for mkdir prototype */
#include <signal.h>
#include <dlfcn.h>
#include <pwd.h>
#include <utime.h> /* for utime prototype */
#endif

#if defined(FLG_SOLARIS_UNIX)
#include <thread.h>
#include <sys/systeminfo.h> /* for sysinfo constants */
#elif defined(FLG_LINUX_UNIX) || defined(FLG_OSX_UNIX)
#include <pthread.h>
#endif

#if defined(FLG_UNIX)
#include <netinet/in.h>         // IPPROTO_IP
#include <sys/socket.h>         // socket()
#include <net/if.h>             // struct ifreq
#include <sys/ioctl.h>          // ioctl() and SIOCGIFHWADDR
#include <sys/ipc.h>
#include <sys/shm.h>
#if defined(FLG_OSX_UNIX)
#include <sys/sysctl.h>
#include <net/if_dl.h>
#include <arpa/inet.h>
#elif defined(FLG_SOLARIS_UNIX)
#include <sys/sockio.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <net/if_arp.h>
#endif
#endif

/*******  GemStone Global constants and types  ******************************/
#include "global.ht"
#include "gemfire.h"
#include "host.hf"
#include "utl.hf"
#include "com_gemstone_gemfire_internal_SmHelper.h"

extern const char *gemfire_version;

JNIEXPORT jint JNICALL Java_com_gemstone_gemfire_internal_SmHelper__1pointerSizeBytes(JNIEnv *env, jclass klass)
{
  return sizeof(intptr_t);
}

/*
 * Class:     com_gemstone_gemfire_internal_SmHelper
 * Method:    _nanosleep
 * Signature: (I)V
 */
JNIEXPORT void JNICALL Java_com_gemstone_gemfire_internal_SmHelper__1nanosleep
  (JNIEnv* env, jclass klass, jint nanos)
{
  HostNanoSleep((uint32)nanos);
}

/*
 * Class:     com_gemstone_gemfire_internal_SmHelper
 * Method:    _getNativeVersion
 * Signature: ()Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_gemstone_gemfire_internal_SmHelper__1getNativeVersion
  (JNIEnv *env, jclass klass)
{
  return (*env)->NewStringUTF(env, gemfire_version);
}

/*
 * Class:     com_gemstone_gemfire_internal_SmHelper
 * Method:    _getSystemId
 * Signature: ()Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_gemstone_gemfire_internal_SmHelper__1getSystemId
  (JNIEnv *env, jclass klass)
{
  unsigned char macAddr[6];
  const char* id;
  char idbuff[256];
#ifdef FLG_UNIX
#if defined(FLG_SOLARIS_UNIX)
  char provider[64];
  char serial[64];
#if 0
  const char* pNIC = "hme0";      // ethernet card device name
  // get mac-address
  memset(macAddr, 0, sizeof(macAddr));
  int fdesc = socket(PF_INET, SOCK_DGRAM, IPPROTO_IP);
  if (fdesc != -1) {
    struct ifreq req;
    strcpy(req.ifr_name, pNIC);
    int err = ioctl(fdesc, SIOCGIFADDR, &req );
    if (err != -1) {
      memcpy(macAddr, req.ifr_addr.sa_data, sizeof(macAddr));
    }
    close(fdesc);
  }
#endif
  char name[MAXHOSTNAMELEN];

  memset(macAddr, 0, sizeof(macAddr));
  if (gethostname(name, sizeof(name)) == 0) {
    struct hostent *phost;
    struct hostent hostentBuf;
    char hbuf[512];
    int herr;
    phost = gethostbyname_r(name, &hostentBuf, hbuf, sizeof(hbuf), &herr);
    if (phost != 0) {
      int s;
      s = socket(PF_INET, SOCK_DGRAM, IPPROTO_UDP);
      if (s != -1) {
        char **paddrs;
        struct arpreq        ar;
        struct sockaddr_in * psa;
        paddrs = phost->h_addr_list;
        psa    = ( struct sockaddr_in * )&( ar.arp_pa );
        memset( &ar, 0, sizeof( struct arpreq ) );
        psa->sin_family = AF_INET;
        memcpy( &( psa->sin_addr ), *paddrs, sizeof( struct in_addr ) );
        if ( ioctl( s, SIOCGARP, &ar ) != -1 ) {
          int i;
          for (i = 0; i < 6; i++) {
            macAddr[i] = ar.arp_ha.sa_data[i];
          }
        }
        close(s);
      }
    }
  }

  sysinfo(SI_HW_PROVIDER, provider, 64);
  sysinfo(SI_HW_SERIAL, serial, 64);

  sprintf(idbuff, "%lx %s-%s %02x-%02x-%02x-%02x-%02x-%02x",
          gethostid(), provider, serial,
          macAddr[0], macAddr[1], macAddr[2],
          macAddr[3], macAddr[4], macAddr[5]);
#elif defined(FLG_OSX_UNIX)
  const char* pNIC = "en0";
  // get mac-address
  memset(macAddr, 0, sizeof(macAddr));
  size_t len;
  char *buf;
  struct if_msghdr        *ifm;
  struct sockaddr_dl      *sdl;

  int mib[6] = {CTL_NET, AF_ROUTE, 0, AF_LINK, NET_RT_IFLIST, 0};
  mib[5] = if_nametoindex(pNIC);
  UTL_ASSERT(mib[5] != 0);
  sysctl(mib, 6, NULL, &len, NULL, 0);
  buf = (char*)UtlMalloc(len, "OBTAIN MAC ADDRESS");
  sysctl(mib, 6, buf, &len, NULL, 0);
#ifdef X86_64
//The following code will have issues on 64bit Mac OS X
+++ error
#endif
  ifm = (struct if_msghdr *)buf;
  sdl = (struct sockaddr_dl *)(ifm + 1);
  memcpy(macAddr, (unsigned char *)LLADDR(sdl), sizeof(macAddr));
  sprintf(idbuff, "%02x-%02x-%02x-%02x-%02x-%02x",
          macAddr[0], macAddr[1], macAddr[2],
          macAddr[3], macAddr[4], macAddr[5]);
#else
  const char* pNIC = "eth0";
  // get mac-address
  memset(macAddr, 0, sizeof(macAddr));
  int fdesc = socket(PF_INET, SOCK_DGRAM, IPPROTO_IP);
  if (fdesc != -1) {
    struct ifreq req;
    strcpy(req.ifr_name, pNIC);
    int err = ioctl(fdesc, SIOCGIFHWADDR, &req );
    if (err != -1) {
      int i;
      struct sockaddr* sa = (struct sockaddr *) &req.ifr_addr;
      for (i = 0; i < 6; i++) {
        macAddr[i] = sa->sa_data[i];
      }
    }
    close(fdesc);
  }
  sprintf(idbuff, "%02x-%02x-%02x-%02x-%02x-%02x",
          macAddr[0], macAddr[1], macAddr[2],
          macAddr[3], macAddr[4], macAddr[5]);
#endif
#else /* windows */
  DWORD serialNumber = 0;
  DWORD maxDirLength;
  DWORD flags;
  if (GetVolumeInformation("c:\\", NULL, 0, &serialNumber, &maxDirLength,
                           &flags, NULL, 0)) {
  }
  {
    int lana_num = -1;		/* LAN adapter number */

    struct ASTAT {
      ADAPTER_STATUS adapt;
      NAME_BUFFER NameBuff[30];
    } Adapter;
    NCB Ncb;

    {
      LANA_ENUM lenum;
      UCHAR uRetCode;
      int li = 0;

      memset(&Ncb,0,sizeof(Ncb));
      Ncb.ncb_command = NCBENUM;
      Ncb.ncb_buffer = (UCHAR*)&lenum;
      Ncb.ncb_length = sizeof(lenum);
      uRetCode = Netbios(&Ncb);
      if (uRetCode != 0) return JNI_FALSE;

      for (; li < lenum.length; li++) {
        lana_num = lenum.lana[li];
        /* prepare to get adapter status block */
        memset(&Ncb, 0, sizeof(Ncb));
        Ncb.ncb_command = NCBRESET;
        Ncb.ncb_lana_num = lana_num;
        if (Netbios(&Ncb) != NRC_GOODRET) continue;

        /* OK, lets go fetch ethernet address */
        memset(&Ncb, 0, sizeof(Ncb));
        Ncb.ncb_command = NCBASTAT;
        Ncb.ncb_lana_num = lana_num;
        strcpy((char *) Ncb.ncb_callname, "*");

        memset(&Adapter, 0, sizeof(Adapter));
        Ncb.ncb_buffer = (unsigned char *)&Adapter;
        Ncb.ncb_length = sizeof(Adapter);
        /* if unable to determine, exit false */
        if (Netbios(&Ncb) != 0) continue;

        /* if correct type, then see if its
           the one we want to check */
        if ((Adapter.adapt.adapter_type & 0xff) == 0xfe) {
          int i;
          for (i = 0; i < 6; i++) {
            macAddr[i] = Adapter.adapt.adapter_address[i];
          }
          break;
        }
      }
    }
  }
  sprintf(idbuff, "%lx %02x-%02x-%02x-%02x-%02x-%02x",
          serialNumber,
          macAddr[0], macAddr[1], macAddr[2],
          macAddr[3], macAddr[4], macAddr[5]);
#endif
  id = idbuff;
  return (*env)->NewStringUTF(env, id);
}

/*
 * Class:     com_gemstone_gemfire_internal_SmHelper
 * Method:    _allocateJOMObject
 * Signature: (Ljava/lang/Class;Ljava/lang/Class;)Ljava/lang/Object;
 *
 * Allocates a new instance of class c and initializes it with the
 * zero-argument constructor of initClass
 */
JNIEXPORT jobject JNICALL Java_com_gemstone_gemfire_internal_SmHelper__1allocateJOMObject
(JNIEnv *env, jclass receiver, jclass c, jclass initClass) {
  jobject obj;
  jmethodID init = (*env)->GetMethodID(env, initClass, "<init>", "()V");

  obj = (*env)->AllocObject(env, c);
  (*env)->CallVoidMethod(env, obj, init);
  return obj;
}

/*
 * Class:     com_gemstone_gemfire_internal_SmHelper
 * Method:    _setObjectField
 * Signature: (Ljava/lang/Object;Ljava/lang/String;Ljava/lang/String;Ljava/lang/Object;)V
 */
JNIEXPORT void JNICALL Java_com_gemstone_gemfire_internal_SmHelper__1setObjectField
(JNIEnv* env, jclass klass, jobject obj, jobject field, jobject newValue) {

  /* look up the JNI field id */
  jfieldID fieldId;
  fieldId = (*env)->FromReflectedField(env, field);

  /* Set the value of the field */
  (*env)->SetObjectField(env, obj, fieldId, newValue);

}
