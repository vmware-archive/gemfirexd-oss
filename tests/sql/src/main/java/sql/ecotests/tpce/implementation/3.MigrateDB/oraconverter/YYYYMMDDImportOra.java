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
import java.sql.Blob;
import java.sql.SQLException;
import java.util.Map;
import java.util.HashMap;

import com.pivotal.gemfirexd.load.ImportBlob;

public class YYYYMMDDImportOra extends com.pivotal.gemfirexd.load.Import {
	public static Map<String, String> monthsMap = new HashMap<String, String>();

	public YYYYMMDDImportOra(String inputFileName, String columnDelimiter,
			String characterDelimiter, String codeset, long offset,
			long endPosition, int noOfColumnsExpected, String columnTypes,
			boolean lobsInExtFile, int importCounter, String columnTypeNames,
			String udtClassNamesString) throws SQLException {
		super(inputFileName, columnDelimiter, characterDelimiter, codeset,
				offset, endPosition, noOfColumnsExpected, columnTypes,
				lobsInExtFile, importCounter, columnTypeNames,
				udtClassNamesString);
	}

	@Override
	public String getString(int columnIndex) throws SQLException {
		String val = super.getString(columnIndex);
		if (val != null && val.length() > 0) {
			switch (getColumnType(columnIndex)) {
			// Oracle date is YYYY-MM-DD
			// GemFireXD format is YYYY-MM-DD HH:MI:SS
			case java.sql.Types.TIMESTAMP:
				return val + " 00:00:00";
			default:
				return val;
			}
		} else {
			return val;
		}
	}

	@Override
	public Blob getBlob(int columnIndex) throws SQLException {
		if (this.lobsInExtFile) {
			return super.getBlob(columnIndex);
		} else {
			// data is in the main export file, stored in hex format.
			String hexData = getCurrentRow()[columnIndex - 1];
			byte[] data = null;
			if (hexData != null) {
				data = fromHexString(hexData, 0, hexData.length());
				// fromHexString() returns null if the hex string
				// is invalid one. It is invalid if the data string
				// length is not multiple of 2 or the data string
				// contains non-hex characters.
				if (data != null) {
					this.wasNull = false;
					return new ImportBlob(data);
				} else {
					throw new SQLException(
							"An invalid hexadecimal string '" + hexData
									+ "' detected in the import file at line "
									+ getCurrentLineNumber() + " column "
									+ columnIndex, "XIE0N", 20000);
				}
			} else {
				this.wasNull = true;
				return null;
			}
		}
	}

	public static byte[] fromHexString(final String s, final int offset,
			int length) {
		int j = 0;
		final byte[] bytes;
		if ((length % 2) != 0) {
			// prepend a zero at the start
			bytes = new byte[(length + 1) >>> 1];
			final int low_nibble = Character.digit(s.charAt(offset), 16);
			if (low_nibble == -1) {
				// illegal format
				return null;
			}
			bytes[j++] = (byte) (low_nibble & 0x0f);
			length--;
		} else {
			bytes = new byte[length >>> 1];
		}

		final int end = offset + length;
		for (int i = offset; i < end; i += 2) {
			final int high_nibble = Character.digit(s.charAt(i), 16);
			final int low_nibble = Character.digit(s.charAt(i + 1), 16);
			if (high_nibble == -1 || low_nibble == -1) {
				// illegal format
				return null;
			}
			bytes[j++] = (byte) (((high_nibble << 4) & 0xf0) | (low_nibble & 0x0f));
		}
		return bytes;
	}
}
