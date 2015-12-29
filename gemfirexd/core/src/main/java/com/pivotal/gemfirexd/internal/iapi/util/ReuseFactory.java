/*

   Derby - Class com.ihost.cs.ReuseFactory

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

/*
 * Changes for GemFireXD distributed data platform (some marked by "GemStone changes")
 *
 * Portions Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
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

package com.pivotal.gemfirexd.internal.iapi.util;

import com.gemstone.gemfire.internal.shared.ClientSharedUtils;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.IndexRowGenerator;

/**
	Factory methods for reusable objects. So far, the objects allocated
	by this factory are all immutable. Any immutable object can be re-used.

	All the methods in this class are static.
*/
public class ReuseFactory {

	/** Private constructor so no instances can be made */
	private ReuseFactory() {
	}

// GemStone changes BEGIN
/*
	private static final Integer[] staticInts =
		{new Integer(0), new Integer(1), new Integer(2), new Integer(3),
		 new Integer(4), new Integer(5), new Integer(6), new Integer(7),
		 new Integer(8), new Integer(9), new Integer(10), new Integer(11),
		 new Integer(12), new Integer(13), new Integer(14), new Integer(15),
		 new Integer(16), new Integer(17), new Integer(18)};
	private static final Integer FIFTY_TWO = new Integer(52);
	private static final Integer TWENTY_THREE = new Integer(23);
	private static final Integer MAXINT = new Integer(Integer.MAX_VALUE);
	private static final Integer MINUS_ONE = new Integer(-1);
 */
// GemStone changes END

	public static Integer getInteger(int i)
	{
// GemStone changes BEGIN
                return Integer.valueOf(i);
	}
	/*
		if (i >= 0 && i < staticInts.length)
		{
			return staticInts[i];
		}
		else
		{
			// Look for other common values
			switch (i)
			{
			  case 23:
				return TWENTY_THREE;	// precision of Int

			  case 52:
				return FIFTY_TWO;	// precision of Double

			  case Integer.MAX_VALUE:
				return MAXINT;

			  case -1:
				return MINUS_ONE;

			  default:
				return new Integer(i);
			}
		}
	}

	private static final Short[] staticShorts =
		{new Short((short) 0), new Short((short) 1), new Short((short) 2),
		 new Short((short) 3), new Short((short) 4), new Short((short) 5),
		 new Short((short) 6), new Short((short) 7), new Short((short) 8),
		 new Short((short) 9), new Short((short) 10)};
	*/
// GemStone changes END

	public static Short getShort(short i)
	{
// GemStone changes BEGIN
	  return Short.valueOf(i);
	}
          /*
		if (i >= 0 && i < staticShorts.length)
			return staticShorts[i];
		else
			return new Short(i);
	}

	private static final Byte[] staticBytes =
		{new Byte((byte) 0), new Byte((byte) 1), new Byte((byte) 2),
		 new Byte((byte) 3), new Byte((byte) 4), new Byte((byte) 5),
		 new Byte((byte) 6), new Byte((byte) 7), new Byte((byte) 8),
		 new Byte((byte) 9), new Byte((byte) 10)};
	*/
// GemStone changes END

	public static Byte getByte(byte i)        
	{
// GemStone changes BEGIN
	  return Byte.valueOf(i);
	}
	/*
		if (i >= 0 && i < staticBytes.length)
			return staticBytes[i];
		else
			return new Byte(i);
	}

	private static final Long[] staticLongs =
		{new Long(0), new Long(1), new Long(2),
		 new Long(3), new Long(4), new Long(5),
		 new Long(6), new Long(7), new Long(8),
		 new Long(9), new Long(10)};
	*/
// GemStone changes END

	public static Long getLong(long i)
	{
// GemStone changes BEGIN
	  return Long.valueOf(i);
	/*
		if (i >= 0 && i < staticLongs.length)
			return staticLongs[(int) i];
		else
			return new Long(i);
	*/
// GemStone changes END
	}

    public static Boolean getBoolean( boolean b)
    {
        return b ? Boolean.TRUE : Boolean.FALSE;
    }

	private static final byte[] staticZeroLenByteArray = new byte[0];
	public static byte[] getZeroLenByteArray() 
	{
		return staticZeroLenByteArray;
	}
        
        private static final int[] staticZeroLenIntArray = new int[0];
// GemStone changes BEGIN
        private static final long[] staticZeroLenLongArray = new long[0];
        private static final IndexRowGenerator[] staticZeroLenIRGArray =
          new IndexRowGenerator[0];

        public static int[] getZeroLenIntArray() {
          return staticZeroLenIntArray;
        }

        public static long[] getZeroLenLongArray() {
          return staticZeroLenLongArray;
        }

        public static Object[] getZeroLenObjectArray() {
          return ClientSharedUtils.getZeroLenObjectArray();
        }

        public static IndexRowGenerator[] getZeroLenIRGArray() {
          return staticZeroLenIRGArray;
        }
// GemStone changes END
}
