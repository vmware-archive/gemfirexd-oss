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
#define UTL_C TRUE
/*******  GemStone Global constants and types  ******************************/
#include "flag.ht"
#include "global.ht"
#include "host.ht"
#include "host.hf"

#include "utl.hf"
#include "host.hf"

/* no more includes. KEEP THIS LINE */

/*========================================================================
 *  This function is used to keep the generated code for the UTL_GUARANTEE 
 * macro as small as possible
 */
EXTERN_GS_DEC(void)
UtlGuaranteeFailure(const char *fileName, int lineNum) {
  printf("UTL_GUARANTEE failure file %s line %d \n", fileName, lineNum);
  HostCallDebuggerMsg("UTL_GUARANTEE failed"); 
}

#ifdef FLG_DEBUG
static int TraceMalloc = 0;
#else
#define TraceMalloc 0
#endif

void* UtlMalloc(int32 aSize, const char* msg)
{
  void *result;
  UTL_ASSERT(aSize >= 0);
  result = malloc(aSize);
  if (result == NULL) {
    HostCallDebuggerMsg("OutOfMemory, malloc failed, no more C heap space");
  }
 
  if (TraceMalloc) {
    if (TraceMalloc > 1)
      printf("UtlMalloc "FMT_P" %d %s \n", result, aSize, msg);
    else
      printf("UtlMalloc "FMT_P"\n", result);
  }
  return result;
}

void* UtlMallocNoErr(int32 aSize, const char* msg)
{
  void *result;
  UTL_ASSERT(aSize > 0);
  result = malloc(aSize);
 
  if (TraceMalloc) {
    if (TraceMalloc > 1)
      printf("UtlMalloc "FMT_P" %d %s \n", result, aSize, msg);
    else
      printf("UtlMalloc "FMT_P"\n", result);
  }
  return result;
}

void* UtlCalloc(size_t nelem, size_t elsize)
{
  void *result = calloc(nelem, elsize);
  if (TraceMalloc) {
    printf("UtlCalloc "FMT_P"\n", result);
  }
  return result;
}

#ifdef FLG_DEBUG
void UtlFree(void *p) 
{
  if (TraceMalloc) {
    printf("Utlmalloc "FMT_P" free\n", p);
  }
  free(p);
}
#endif
