/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.drda.CodePointNameTable

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
package com.pivotal.gemfirexd.internal.impl.drda;

/**
  This class has a hashtable of CodePoint values.  It is used by the tracing
  code and by the protocol testing code
  It is arranged in alphabetical order.
*/

class CodePointNameTable extends java.util.Hashtable
{
  CodePointNameTable ()
  {
    put (Integer.valueOf(CodePoint.ABNUOWRM), "ABNUOWRM");
    put (Integer.valueOf(CodePoint.ACCRDB), "ACCRDB");
    put (Integer.valueOf(CodePoint.ACCRDBRM), "ACCRDBRM");
    put (Integer.valueOf(CodePoint.ACCSEC), "ACCSEC");
    put (Integer.valueOf(CodePoint.ACCSECRD), "ACCSECRD");
    put (Integer.valueOf(CodePoint.AGENT), "AGENT");
    put (Integer.valueOf(CodePoint.AGNPRMRM), "AGNPRMRM");
    put (Integer.valueOf(CodePoint.BGNBND), "BGNBND");
    put (Integer.valueOf(CodePoint.BGNBNDRM), "BGNBNDRM");
    put (Integer.valueOf(CodePoint.BNDSQLSTT), "BNDSQLSTT");
    put (Integer.valueOf(CodePoint.CCSIDSBC), "CCSIDSBC");
    put (Integer.valueOf(CodePoint.CCSIDMBC), "CCSIDMBC");
    put (Integer.valueOf(CodePoint.CCSIDDBC), "CCSIDDBC");
    put (Integer.valueOf(CodePoint.CLSQRY), "CLSQRY");
    put (Integer.valueOf(CodePoint.CMDATHRM), "CMDATHRM");
    put (Integer.valueOf(CodePoint.CMDCHKRM), "CMDCHKRM");
    put (Integer.valueOf(CodePoint.CMDCMPRM), "CMDCMPRM");
    put (Integer.valueOf(CodePoint.CMDNSPRM), "CMDNSPRM");
    put (Integer.valueOf(CodePoint.CMMRQSRM), "CMMRQSRM");
    put (Integer.valueOf(CodePoint.CMDVLTRM), "CMDVLTRM");
    put (Integer.valueOf(CodePoint.CNTQRY), "CNTQRY");
    put (Integer.valueOf(CodePoint.CRRTKN), "CRRTKN");
    put (Integer.valueOf(CodePoint.DRPPKG), "DRPPKG");
    put (Integer.valueOf(CodePoint.DSCRDBTBL), "DSCRDBTBL");
    put (Integer.valueOf(CodePoint.DSCINVRM), "DSCINVRM");
    put (Integer.valueOf(CodePoint.DSCSQLSTT), "DSCSQLSTT");
    put (Integer.valueOf(CodePoint.DTAMCHRM), "DTAMCHRM");
    put (Integer.valueOf(CodePoint.ENDBND), "ENDBND");
    put (Integer.valueOf(CodePoint.ENDQRYRM), "ENDQRYRM");
    put (Integer.valueOf(CodePoint.ENDUOWRM), "ENDUOWRM");
    put (Integer.valueOf(CodePoint.EXCSAT), "EXCSAT");
    put (Integer.valueOf(CodePoint.EXCSATRD), "EXCSATRD");
    put (Integer.valueOf(CodePoint.EXCSQLIMM), "EXCSQLIMM");
    put (Integer.valueOf(CodePoint.EXCSQLSET), "EXCSQLSET");
    put (Integer.valueOf(CodePoint.EXCSQLSTT), "EXCSQLSTT");
    put (Integer.valueOf(CodePoint.EXTNAM), "EXTNAM");
    put (Integer.valueOf(CodePoint.FRCFIXROW), "FRCFIXROW");
    put (Integer.valueOf(CodePoint.MAXBLKEXT), "MAXBLKEXT");
    put (Integer.valueOf(CodePoint.MAXRSLCNT), "MAXRSLCNT");
    put (Integer.valueOf(CodePoint.MGRDEPRM), "MGRDEPRM");
    put (Integer.valueOf(CodePoint.MGRLVLLS), "MGRLVLLS");
    put (Integer.valueOf(CodePoint.MGRLVLRM), "MGRLVLRM");
    put (Integer.valueOf(CodePoint.MONITOR), "MONITOR");
    put (Integer.valueOf(CodePoint.NBRROW), "NBRROW");
    put (Integer.valueOf(CodePoint.OBJNSPRM), "OBJNSPRM");
    put (Integer.valueOf(CodePoint.OPNQFLRM), "OPNQFLRM");
    put (Integer.valueOf(CodePoint.OPNQRY), "OPNQRY");
    put (Integer.valueOf(CodePoint.OPNQRYRM), "OPNQRYRM");
    put (Integer.valueOf(CodePoint.OUTEXP), "OUTEXP");
    put (Integer.valueOf(CodePoint.OUTOVR), "OUTOVR");
    put (Integer.valueOf(CodePoint.OUTOVROPT), "OUTOVROPT");
    put (Integer.valueOf(CodePoint.PASSWORD), "PASSWORD");
    put (Integer.valueOf(CodePoint.PKGID), "PKGID");
    put (Integer.valueOf(CodePoint.PKGBNARM), "PKGBNARM");
    put (Integer.valueOf(CodePoint.PKGBPARM), "PKGBPARM");
    put (Integer.valueOf(CodePoint.PKGNAMCSN), "PKGNAMCSN");
    put (Integer.valueOf(CodePoint.PKGNAMCT), "PKGNAMCT");
    put (Integer.valueOf(CodePoint.PRCCNVRM), "PRCCNVRM");
    put (Integer.valueOf(CodePoint.PRDID), "PRDID");
    put (Integer.valueOf(CodePoint.PRDDTA), "PRDDTA");
    put (Integer.valueOf(CodePoint.PRMNSPRM), "PRMNSPRM");
    put (Integer.valueOf(CodePoint.PRPSQLSTT), "PRPSQLSTT");
    put (Integer.valueOf(CodePoint.QRYBLKCTL), "QRYBLKCTL");
    put (Integer.valueOf(CodePoint.QRYBLKRST), "QRYBLKRST");
    put (Integer.valueOf(CodePoint.QRYBLKSZ), "QRYBLKSZ");
    put (Integer.valueOf(CodePoint.QRYCLSIMP), "QRYCLSIMP");
    put (Integer.valueOf(CodePoint.QRYCLSRLS), "QRYCLSRLS");
    put (Integer.valueOf(CodePoint.QRYDSC), "QRYDSC");
    put (Integer.valueOf(CodePoint.QRYDTA), "QRYDTA");
    put (Integer.valueOf(CodePoint.QRYINSID), "QRYINSID");
    put (Integer.valueOf(CodePoint.QRYNOPRM), "QRYNOPRM");
    put (Integer.valueOf(CodePoint.QRYPOPRM), "QRYPOPRM");
    put (Integer.valueOf(CodePoint.QRYRELSCR), "QRYRELSCR");
    put (Integer.valueOf(CodePoint.QRYRFRTBL), "QRYRFRTBL");
    put (Integer.valueOf(CodePoint.QRYROWNBR), "QRYROWNBR");
    put (Integer.valueOf(CodePoint.QRYROWSNS), "QRYROWSNS");
    put (Integer.valueOf(CodePoint.QRYRTNDTA), "QRYRTNDTA");
    put (Integer.valueOf(CodePoint.QRYSCRORN), "QRYSCRORN");
    put (Integer.valueOf(CodePoint.QRYROWSET), "QRYROWSET");
    put (Integer.valueOf(CodePoint.RDBAFLRM), "RDBAFLRM");
    put (Integer.valueOf(CodePoint.RDBACCCL), "RDBACCCL");
    put (Integer.valueOf(CodePoint.RDBACCRM), "RDBACCRM");
    put (Integer.valueOf(CodePoint.RDBALWUPD), "RDBALWUPD");
    put (Integer.valueOf(CodePoint.RDBATHRM), "RDBATHRM");
    put (Integer.valueOf(CodePoint.RDBCMM), "RDBCMM");
    put (Integer.valueOf(CodePoint.RDBCMTOK), "RDBCMTOK");
    put (Integer.valueOf(CodePoint.RDBNACRM), "RDBNACRM");
    put (Integer.valueOf(CodePoint.RDBNAM), "RDBNAM");
    put (Integer.valueOf(CodePoint.RDBNFNRM), "RDBNFNRM");
    put (Integer.valueOf(CodePoint.RDBRLLBCK), "RDBRLLBCK");
    put (Integer.valueOf(CodePoint.RDBUPDRM), "RDBUPDRM");
    put (Integer.valueOf(CodePoint.REBIND), "REBIND");
    put (Integer.valueOf(CodePoint.RSCLMTRM), "RSCLMTRM");
    put (Integer.valueOf(CodePoint.RSLSETRM), "RSLSETRM");
    put (Integer.valueOf(CodePoint.RTNEXTDTA), "RTNEXTDTA");
    put (Integer.valueOf(CodePoint.RTNSQLDA), "RTNSQLDA");
    put (Integer.valueOf(CodePoint.SECCHK), "SECCHK");
    put (Integer.valueOf(CodePoint.SECCHKCD), "SECCHKCD");
    put (Integer.valueOf(CodePoint.SECCHKRM), "SECCHKRM");
    put (Integer.valueOf(CodePoint.SECMEC), "SECMEC");
    put (Integer.valueOf(CodePoint.SECMGRNM), "SECMGRNM");
    put (Integer.valueOf(CodePoint.SECTKN), "SECTKN");
    put (Integer.valueOf(CodePoint.SPVNAM), "SPVNAM");
    put (Integer.valueOf(CodePoint.SQLAM), "SQLAM");
    put (Integer.valueOf(CodePoint.SQLATTR), "SQLATTR");
    put (Integer.valueOf(CodePoint.SQLCARD), "SQLCARD");
    put (Integer.valueOf(CodePoint.SQLERRRM), "SQLERRRM");
    put (Integer.valueOf(CodePoint.SQLDARD), "SQLDARD");
    put (Integer.valueOf(CodePoint.SQLDTA), "SQLDTA");
    put (Integer.valueOf(CodePoint.SQLDTARD), "SQLDTARD");
    put (Integer.valueOf(CodePoint.SQLSTT), "SQLSTT");
    put (Integer.valueOf(CodePoint.SQLSTTVRB), "SQLSTTVRB");
    put (Integer.valueOf(CodePoint.SRVCLSNM), "SRVCLSNM");
    put (Integer.valueOf(CodePoint.SRVRLSLV), "SRVRLSLV");
    put (Integer.valueOf(CodePoint.SRVNAM), "SRVNAM");
    put (Integer.valueOf(CodePoint.SVRCOD), "SVRCOD");
    put (Integer.valueOf(CodePoint.SYNCCTL), "SYNCCTL");
    put (Integer.valueOf(CodePoint.SYNCLOG), "SYNCLOG");
    put (Integer.valueOf(CodePoint.SYNCRSY), "SYNCRSY");
    put (Integer.valueOf(CodePoint.SYNTAXRM), "SYNTAXRM");
    put (Integer.valueOf(CodePoint.TRGNSPRM), "TRGNSPRM");
    put (Integer.valueOf(CodePoint.TYPDEFNAM), "TYPDEFNAM");
    put (Integer.valueOf(CodePoint.TYPDEFOVR), "TYPDEFOVR");
    put (Integer.valueOf(CodePoint.TYPSQLDA), "TYPSQLDA");
    put (Integer.valueOf(CodePoint.UOWDSP), "UOWDSP");
    put (Integer.valueOf(CodePoint.USRID), "USRID");
    put (Integer.valueOf(CodePoint.VALNSPRM), "VALNSPRM");
    put (Integer.valueOf(CodePoint.PBSD), "PBSD");
    put (Integer.valueOf(CodePoint.PBSD_ISO), "PBSD_ISO");
    put (Integer.valueOf(CodePoint.PBSD_SCHEMA), "PBSD_SCHEMA");
  }

  String lookup (int codePoint)
  {
    return (String) get (Integer.valueOf(codePoint));
  }

}
