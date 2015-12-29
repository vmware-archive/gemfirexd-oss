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
#include <time.h>
#include <stdio.h>
#include <jni.h>
#include <com_gemstone_gemfire_internal_NanoTimer.h>
#include <com_pivotal_gemfirexd_internal_GemFireXDVersion.h>

#ifdef FLG_DEBUG
const char* gemfirexd_version = GEMFIRE_VERSION " " GEMFIRE_BUILDID " " GEMFIRE_BUILDDATE " debug " GEMFIRE_BUILDOS;
#else
const char* gemfirexd_version = GEMFIRE_VERSION " " GEMFIRE_BUILDID " " GEMFIRE_BUILDDATE " optimized " GEMFIRE_BUILDOS;
#endif

#if defined(FLG_MSWIN32)
static jclass systemClassId;
static jmethodID systemNanoTimeMethodId;
#endif

/*
JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM *vm, void *reserved) {
#if !defined(FLG_MSWIN32)
    initUtilLibrary();
#endif

    return JNI_VERSION_1_4;
}
*/

JNIEXPORT jlong JNICALL Java_com_gemstone_gemfire_internal_NanoTimer__1nanoTime(
        JNIEnv * env, jclass klazz, jint clock_type) {
#if !defined(FLG_MSWIN32)
    struct timespec ts;

    // fail safe to ensure we don't pass an invalid clock_id to the kernel.
    // observed some crashes in the kernel call which takes out the jvm altogether.
    int clk_id = (int) clock_type;
    switch (clk_id) {
        case CLOCK_REALTIME:
        case CLOCK_MONOTONIC:
        case CLOCK_PROCESS_CPUTIME_ID:
        case CLOCK_THREAD_CPUTIME_ID:
        case CLOCK_MONOTONIC_RAW:
            clock_gettime((clock_t)clk_id, &ts);
            break;
        default:
            // fallback to MONOTONIC
            clock_gettime(CLOCK_MONOTONIC, &ts);
            break;
    }

    return ((jlong)ts.tv_sec) * (1000000000LL) + ((jlong)ts.tv_nsec);

#else
    /* using java.lang.System.nanoTime on other platforms */
    if (systemClassId == 0) {
        systemClassId = (*env)->FindClass(env, "java/lang/System");
        systemNanoTimeMethodId = (*env)->GetStaticMethodID(env, systemClassId , "nanoTime", "()J");
    }

    if (systemNanoTimeMethodId == 0) {
        return 0;
    }

    return (*env)->CallStaticLongMethod(env, systemClassId, systemNanoTimeMethodId);
#endif
}

/*
 * Class:     com_pivotal_gemfirexd_internal_GemFireXDVersion
 * Method:    _getNativeVersion
 * Signature: ()Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_pivotal_gemfirexd_internal_GemFireXDVersion__1getNativeVersion(
        JNIEnv * env, jclass klazz) {
    return (*env)->NewStringUTF(env, gemfirexd_version);
}

//clock_getres(CLOCK_MONOTONIC,&ts);
//printf("%ld", ts.tv_nsec);

/*
 int main()
 {
 timespec ts, ts1;
 clock_getres(CLOCK_MONOTONIC,&ts);
 printf("%ld", ts.tv_nsec);

 // clock_gettime(CLOCK_MONOTONIC, &ts); // Works on FreeBSD
 for ( int i = 0; i < 900000; i++) {
 clock_gettime(CLOCK_MONOTONIC, &ts); // Works on Linux

 clock_gettime(CLOCK_MONOTONIC, &ts1); // Works on Linux
 double diff = ts1.tv_nsec - ts.tv_nsec;

 if (diff > 0)
 printf("%lf\n", diff);
 }
 }
 */
