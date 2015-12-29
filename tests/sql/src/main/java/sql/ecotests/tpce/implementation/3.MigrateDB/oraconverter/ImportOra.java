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

public class ImportOra extends com.pivotal.gemfirexd.load.Import {
	private static Map<String, String> monthsMap = new HashMap<String, String>();

	public ImportOra(String inputFileName, String columnDelimiter,
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
	public Blob getBlob(int columnIndex) throws SQLException {
		if (this.lobsInExtFile) {
			return super.getBlob(columnIndex);
		} else {
			// data is in the main export file and created as ASCII char in C++.
			String ansiCharData = getCurrentRow()[columnIndex - 1];
			byte[] data = null;
			if (ansiCharData != null) {
				data = ansiCharData.getBytes();
				if (data != null) {
					this.wasNull = false;
					return new ImportBlob(data);
				} else {
					throw new SQLException(
							"An invalid ASCII string '" + ansiCharData
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
}
