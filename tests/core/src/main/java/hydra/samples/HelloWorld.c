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
#include <stdio.h>
#include <stdlib.h>

int main( int argc, char**argv ) {

   int i, iterations;

   if ( argc != 2 ) {
     fprintf( stderr, "Usage: HelloWorld <iterations>\n" );
     exit(1);
   }
   iterations = atoi( argv[1] );
   for ( i = 0; i < iterations; i++ ) {
     fprintf( stdout, "%d: hello world out\n", i );
     fprintf( stderr, "%d: hello world err\n", i );
     fflush( stdout ); fflush( stderr );
   }
   exit(0);
}
