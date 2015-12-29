/*

   Derby - Class com.pivotal.gemfirexd.internal.client.net.CodePointNameTable

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
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

package com.pivotal.gemfirexd.internal.client.net;

// GemStone changes BEGIN
import com.gemstone.gemfire.internal.shared.ClientSharedUtils;
import com.gemstone.gemfire.internal.shared.JdkHelper;

// GemStone changes END
// This mapping is used by DssTrace only.
// This is not part of the driver and is not initialized unless dss tracing is enabled.
// This is an abstract mapping from 2-byte code point to a string representing the name of the code point.
// This data type may be modified for performance to adapt to any sort of lookup implementation,
// such as binary search on an underlying sorted array.

class CodePointNameTable extends java.util.Hashtable {
    CodePointNameTable() {
// GemStone changes BEGIN
        // changed to use JdkHelper.newInteger() instead of new Integer
        final JdkHelper helper = ClientSharedUtils.getJdkHelper();
// GemStone changes END
        put(helper.newInteger(CodePoint.ACCSECRD), "ACCSECRD");
        put(helper.newInteger(CodePoint.TYPDEFNAM), "TYPDEFNAM");
        put(helper.newInteger(CodePoint.TYPDEFOVR), "TYPDEFOVR");
        put(helper.newInteger(CodePoint.EXCSAT), "EXCSAT");
        put(helper.newInteger(CodePoint.SYNCCTL), "SYNCCTL");
        put(helper.newInteger(CodePoint.SYNCCRD), "SYNCCRD");
        put(helper.newInteger(CodePoint.SYNCRSY), "SYNCRSY");
        put(helper.newInteger(CodePoint.ACCSEC), "ACCSEC");
        put(helper.newInteger(CodePoint.SECCHK), "SECCHK");
        put(helper.newInteger(CodePoint.MGRLVLRM), "MGRLVLRM");
        put(helper.newInteger(CodePoint.SECCHKRM), "SECCHKRM");
        put(helper.newInteger(CodePoint.CMDNSPRM), "CMDNSPRM");
        put(helper.newInteger(CodePoint.OBJNSPRM), "OBJNSPRM");
        put(helper.newInteger(CodePoint.CMDCHKRM), "CMDCHKRM");
        put(helper.newInteger(CodePoint.SYNTAXRM), "SYNTAXRM");
        put(helper.newInteger(CodePoint.VALNSPRM), "VALNSPRM");
        put(helper.newInteger(CodePoint.EXCSATRD), "EXCSATRD");
        put(helper.newInteger(CodePoint.ACCRDB), "ACCRDB");
        put(helper.newInteger(CodePoint.CLSQRY), "CLSQRY");
        put(helper.newInteger(CodePoint.CNTQRY), "CNTQRY");
        put(helper.newInteger(CodePoint.DSCSQLSTT), "DSCSQLSTT");
        put(helper.newInteger(CodePoint.EXCSQLIMM), "EXCSQLIMM");
        put(helper.newInteger(CodePoint.EXCSQLSTT), "EXCSQLSTT");
        put(helper.newInteger(CodePoint.OPNQRY), "OPNQRY");
        put(helper.newInteger(CodePoint.OUTOVR), "OUTOVR");
        put(helper.newInteger(CodePoint.PRPSQLSTT), "PRPSQLSTT");
        put(helper.newInteger(CodePoint.RDBCMM), "RDBCMM");
        put(helper.newInteger(CodePoint.RDBRLLBCK), "RDBRLLBCK");
        put(helper.newInteger(CodePoint.DSCRDBTBL), "DSCRDBTBL");
        put(helper.newInteger(CodePoint.ACCRDBRM), "ACCRDBRM");
        put(helper.newInteger(CodePoint.QRYNOPRM), "QRYNOPRM");
        put(helper.newInteger(CodePoint.RDBATHRM), "RDBATHRM");
        put(helper.newInteger(CodePoint.RDBNACRM), "RDBNACRM");
        put(helper.newInteger(CodePoint.OPNQRYRM), "OPNQRYRM");
        put(helper.newInteger(CodePoint.RDBACCRM), "RDBACCRM");
        put(helper.newInteger(CodePoint.ENDQRYRM), "ENDQRYRM");
        put(helper.newInteger(CodePoint.ENDUOWRM), "ENDUOWRM");
        put(helper.newInteger(CodePoint.ABNUOWRM), "ABNUOWRM");
        put(helper.newInteger(CodePoint.DTAMCHRM), "DTAMCHRM");
        put(helper.newInteger(CodePoint.QRYPOPRM), "QRYPOPRM");
        put(helper.newInteger(CodePoint.RDBNFNRM), "RDBNFNRM");
        put(helper.newInteger(CodePoint.OPNQFLRM), "OPNQFLRM");
        put(helper.newInteger(CodePoint.SQLERRRM), "SQLERRRM");
        put(helper.newInteger(CodePoint.RDBUPDRM), "RDBUPDRM");
        put(helper.newInteger(CodePoint.RSLSETRM), "RSLSETRM");
        put(helper.newInteger(CodePoint.RDBAFLRM), "RDBAFLRM");
        put(helper.newInteger(CodePoint.SQLCARD), "SQLCARD");
        put(helper.newInteger(CodePoint.SQLDARD), "SQLDARD");
        put(helper.newInteger(CodePoint.SQLDTA), "SQLDTA");
        put(helper.newInteger(CodePoint.SQLDTARD), "SQLDTARD");
        put(helper.newInteger(CodePoint.SQLSTT), "SQLSTT");
        put(helper.newInteger(CodePoint.QRYDSC), "QRYDSC");
        put(helper.newInteger(CodePoint.QRYDTA), "QRYDTA");
        put(helper.newInteger(CodePoint.PRCCNVRM), "PRCCNVRM");
        put(helper.newInteger(CodePoint.EXCSQLSET), "EXCSQLSET");
        put(helper.newInteger(CodePoint.EXTDTA), "EXTDTA");
        put(helper.newInteger(CodePoint.PBSD), "PBSD");
        put(helper.newInteger(CodePoint.PBSD_ISO), "PBSD_ISO");
        put(helper.newInteger(CodePoint.PBSD_SCHEMA), "PBSD_SCHEMA");
    }

    String lookup(int codePoint) {
        return (String) get(ClientSharedUtils.getJdkHelper()
            .newInteger(codePoint));
    }
}
