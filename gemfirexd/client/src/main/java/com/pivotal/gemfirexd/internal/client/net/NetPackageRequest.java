/*

   Derby - Class com.pivotal.gemfirexd.internal.client.net.NetPackageRequest

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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.gemstone.gemfire.internal.shared.ClientSharedData;
import com.pivotal.gemfirexd.internal.client.am.Configuration;
import com.pivotal.gemfirexd.internal.client.am.Section;
import com.pivotal.gemfirexd.internal.client.am.SqlException;
import com.pivotal.gemfirexd.internal.client.am.ClientMessageId;
import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;


public class NetPackageRequest extends NetConnectionRequest {
    static final String COLLECTIONNAME = "NULLID";

    NetPackageRequest(NetAgent netAgent, CcsidManager ccsidManager, int bufferSize) {
        super(netAgent, ccsidManager, bufferSize);
    }

    // RDB Package Name, Consistency Token
    // Scalar Object specifies the fully qualified name of a relational
    // database package and its consistency token.
    //
    // To accomodate larger lengths, the Scalar Data Length
    // (SCLDTALEN) Field is used to specify the length of the instance
    // variable which follows.
    static final String collectionName = "NULLID";

    void buildCommonPKGNAMinfo(Section section) throws SqlException {
        String collectionToFlow = COLLECTIONNAME;
        // the scalar data length field may or may not be required.  it depends
        // on the level of support and length of the data.
        // check the lengths of the RDBNAM, RDBCOLID, and PKGID.
        // Determine if the lengths require an SCLDTALEN object.
        // Note: if an SQLDTALEN is required for ONE of them,
        // it is needed for ALL of them.  This is why this check is
        // up front.
        // the SQLAM level dictates the maximum size for
        // RDB Collection Identifier (RDBCOLID)
        // Relational Database Name (RDBNAM)
        // RDB Package Identifier (PKGID)
        int maxIdentifierLength = NetConfiguration.PKG_IDENTIFIER_MAX_LEN;

        boolean scldtalenRequired = false;
        scldtalenRequired = checkPKGNAMlengths(netAgent_.netConnection_.databaseName_,
                maxIdentifierLength,
                NetConfiguration.PKG_IDENTIFIER_FIXED_LEN);

        if (!scldtalenRequired) {
            scldtalenRequired = checkPKGNAMlengths(collectionToFlow,
                    maxIdentifierLength,
                    NetConfiguration.PKG_IDENTIFIER_FIXED_LEN);
        }

        if (!scldtalenRequired) {
            scldtalenRequired = checkPKGNAMlengths(section.getPackageName(),
                    maxIdentifierLength,
                    NetConfiguration.PKG_IDENTIFIER_FIXED_LEN);
        }

        // the format is different depending on if an SCLDTALEN is required.
        if (!scldtalenRequired) {
            writeScalarPaddedString(netAgent_.netConnection_.databaseName_,
                    NetConfiguration.PKG_IDENTIFIER_FIXED_LEN);
            writeScalarPaddedString(collectionToFlow,
                    NetConfiguration.PKG_IDENTIFIER_FIXED_LEN);
            writeScalarPaddedString(section.getPackageName(),
                    NetConfiguration.PKG_IDENTIFIER_FIXED_LEN);
        } else {
            buildSCLDTA(netAgent_.netConnection_.databaseName_, NetConfiguration.PKG_IDENTIFIER_FIXED_LEN);
            buildSCLDTA(collectionToFlow, NetConfiguration.PKG_IDENTIFIER_FIXED_LEN);
            buildSCLDTA(section.getPackageName(), NetConfiguration.PKG_IDENTIFIER_FIXED_LEN);
        }
    }

    private void buildSCLDTA(String identifier, int minimumLength) throws SqlException {
        if (identifier.length() <= minimumLength) {
            write2Bytes(minimumLength);
            writeScalarPaddedString(identifier, minimumLength);
        } else {
            write2Bytes(identifier.length());
            writeScalarPaddedString(identifier, identifier.length());
        }
    }


    private static ArrayList EMPTY_BUCKET_LIST = new ArrayList();
    
    // GemStone changes begin
    void buildPKGNAMCSN(Section section) throws SqlException {
      buildPKGNAMCSN2(section, true);
    }
    // GemStone changes end
    // this specifies the fully qualified package name,
    // consistency token, and section number within the package being used
    // to execute the SQL.  If the connection supports reusing the previous
    // package information and this information is the same except for the section
    // number then only the section number needs to be sent to the server.
    // GemStone changes BEGIN
    // changing the name of the method from buildPKGNAMCSN to buildPKGNAMCSN2
    void buildPKGNAMCSN2(Section section, boolean writeShopSection) throws SqlException {
    // GemStone changes END
      // Gemstone changes BEGIN
      final boolean isCopiedSection = section.isThisACopiedSection() && writeShopSection;
      final String sql = section.getSQLString();
      String regionName = null;
      int codepoint = CodePoint.PKGNAMCSN;
      if (sql != null && writeShopSection) {
        codepoint = CodePoint.PKGNAMCSN_SHOP_WITH_STR;
        regionName = section.getRegionName();
        section.setSqlStringToNull();
      }
      HashSet bucketIds = null;
      if (isCopiedSection) { 
        bucketIds = section.getBucketIds();
        codepoint = CodePoint.PKGNAMCSN_SHOP;
      }
      if (SanityManager.TraceSingleHop) {
        SanityManager.DEBUG_PRINT(
            SanityManager.TRACE_SINGLE_HOP,
            "NetPackageRequest::buildPKGNAMCSN codepoint: " + codepoint
                + ", section object: " + section + ", isCopiedSection: "
                + isCopiedSection + " server location: "
                + this.netAgent_.getServerLocation() + ", sql: " + sql);
      }
      // Gemstone changes END
        if (!canCommandUseDefaultPKGNAMCSN()) {
            // Gemstone changes BEGIN
            /* original code
            markLengthBytes(CodePoint.PKGNAMCSN);
            */
            markLengthBytes(codepoint);
            // Gemstone changes END
            // If PKGNAMCBytes is already available, copy the bytes to the request buffer directly.
            if (section.getPKGNAMCBytes() != null) {
                writeStoredPKGNAMCBytes(section);
            } else {
                // Mark the beginning of PKGNAMCSN bytes.
                markForCachingPKGNAMCSN();
                buildCommonPKGNAMinfo(section);
                writeScalarPaddedBytes(Configuration.dncPackageConsistencyToken,
                        NetConfiguration.PKGCNSTKN_FIXED_LEN,
                        NetConfiguration.NON_CHAR_DDM_DATA_PAD_BYTE);
                // store the PKGNAMCbytes
                storePKGNAMCBytes(section);
            }
            write2Bytes(section.getSectionNumber());
            updateLengthBytes();
            int execSeq = -1;
            if (section != null) {
              execSeq = section.getExecutionSequence();
            }
            // Null check is enough as we are bringing the implicit savepoint and server version together in GFXD2.0
            if (netAgent_.netConnection_.serverVersion != null) {
              write2Bytes(execSeq);
            }
         // Gemstone changes BEGIN
         if (isCopiedSection) {
           int sz = bucketIds.size();
           write2Bytes(sz);
           Iterator itr = bucketIds.iterator();
           while(itr.hasNext()) {
             int bid = ((Integer)itr.next()).intValue();
             write2Bytes(bid);
           }
          TxID txid = (netAgent_.netConnection_.serverVersion != null && section
              .txIdToBeSent()) ? section.getParentTxID() : null;
           if (txid != null) {
             writeByte(ClientSharedData.CLIENT_TXID_WRITTEN);
             int il;
            try {
              il = netAgent_.netConnection_.getTransactionIsolation();
            } catch (SQLException e) {
              throw new SqlException(e);
            }
            writeInt(il);
            writeLong(txid.getMemberId());
            writeLong(txid.getUniqId());
            if (SanityManager.TraceSingleHop) {
              SanityManager.DEBUG_PRINT(SanityManager.TRACE_SINGLE_HOP,
                  "NetPackageRequest::buildPKGNAMCSN txn info written txid="
                      + txid + ", isolation level=" + il);
            }
           }
           else {
             writeByte(ClientSharedData.CLIENT_TXID_NOT_WRITTEN);
           }
         } else if (codepoint == CodePoint.PKGNAMCSN_SHOP_WITH_STR) {
           // write the region name as string
           buildNOCMorNOCS(regionName);
           section.setRegionName(null);
         }
         // Gemstone changes END
           //updateLengthBytes();
        } else {
            writeScalar2Bytes(CodePoint.PKGSN, section.getSectionNumber());
        }
    }

    private void storePKGNAMCBytes(Section section) {
        // Get the locaton where we started writing PKGNAMCSN
        int startPos = popMarkForCachingPKGNAMCSN();
        int copyLength = offset_ - startPos;
        byte[] b = new byte[copyLength];
        System.arraycopy(bytes_,
                startPos,
                b,
                0,
                copyLength);
        section.setPKGNAMCBytes(b);
    }

    private void writeStoredPKGNAMCBytes(Section section) {
        byte[] b = section.getPKGNAMCBytes();

        // Mare sure request buffer has enough space to write this byte array.
        ensureLength(offset_ + b.length);

        System.arraycopy(b,
                0,
                bytes_,
                offset_,
                b.length);

        offset_ += b.length;
    }

    private boolean canCommandUseDefaultPKGNAMCSN() {
        return false;
    }


    // throws an exception if lengths exceed the maximum.
    // returns a boolean indicating if SLCDTALEN is required.
    private boolean checkPKGNAMlengths(String identifier,
                                       int maxIdentifierLength,
                                       int lengthRequiringScldta) throws SqlException {
        int length = identifier.length();
        if (length > maxIdentifierLength) {
            throw new SqlException(netAgent_.logWriter_,
                new ClientMessageId(SQLState.LANG_IDENTIFIER_TOO_LONG),
                identifier, new Integer(maxIdentifierLength));
        }

        return (length > lengthRequiringScldta);
    }

    private byte[] getBytes(String string, String encoding) throws SqlException {
        try {
            return string.getBytes(encoding);
        } catch (java.lang.Exception e) {
            throw new SqlException(netAgent_.logWriter_, 
                new ClientMessageId(SQLState.JAVA_EXCEPTION), 
                e.getClass().getName(), e.getMessage(), e);
        }
    }

    private void buildNOCMorNOCS(String string) throws SqlException {
// GemStone changes BEGIN
      buildNOCMorNOCS(string, false);
    };

    private void buildNOCMorNOCS(String string, boolean writeLength)
        throws SqlException {
      if (string == null) {
        write2Bytes(0xffff);
        if (writeLength) {
          int lengthLocation = popMark();
          int length = 2 /* for codepoint */+ 2 /* length */+ 2;
          // write the 2 byte length field (2 bytes before codepoint)
          bytes_[lengthLocation] = (byte)((length >>> 8) & 0xff);
          bytes_[lengthLocation + 1] = (byte)(length & 0xff);
        }
        return;
      }
  
      boolean hasNullTermination = true;
      final byte[] sqlBytes = getBytes(string,
          netAgent_.typdef_.getCcsidMbcEncoding());
      int sqlLen = sqlBytes.length;

      int length = sqlLen + 2 /* for codepoint */+ 2 /* length itself */
          + 1 + 1 + 4;
      final boolean needsExtendedLength =
          ((sqlLen + offset_ - dssLengthLocation_) > DssConstants.MAX_DSS_LEN);
      // write the length (including any extended length below)

      if (writeLength) {
        // remove the top length location offset from the mark stack
        // calculate the length based on the marked location and end of data.
        int lengthLocation = popMark();

        // determine if any extended length bytes are needed. the value returned
        // from calculateExtendedLengthByteCount is the number of extended
        // length bytes required. 0 indicates no exteneded length.
        if (needsExtendedLength) {

          int extendedLengthByteCount = calculateExtendedLengthByteCount(
              length);
          if (extendedLengthByteCount == 0) {
            extendedLengthByteCount = 4;
          }
          // ensure there is enough room in the buffer for the extended length
          // bytes.
          ensureLength(offset_ + extendedLengthByteCount);
  
          // calculate the length to be placed in the extended length bytes.
          // this length does not include the 4 byte llcp.
          int extendedLength = length - 4;
  
          // write the extended length
          int shiftSize = (extendedLengthByteCount - 1) * 8;
          for (int i = 0; i < extendedLengthByteCount; i++) {
            bytes_[offset_++] = (byte)((extendedLength >>> shiftSize) & 0xff);
            shiftSize -= 8;
          }
          // the two byte length field before the codepoint contains the length
          // of itself, the length of the codepoint, and the number of bytes
          // used to hold the extended length. the 2 byte length field also has
          // the first bit on to indicate extended length bytes were used.
          length = extendedLengthByteCount + 4;
          length |= 0x8000;

          // finalize the DSS length to indicate continuation header
          final int totalSize = DssConstants.MAX_DSS_LEN + 1;
          bytes_[dssLengthLocation_] = (byte) ((totalSize >>> 8) & 0xff);
          bytes_[dssLengthLocation_ + 1] = (byte) (totalSize & 0xff);
        }

        // write the 2 byte length field (2 bytes before codepoint)
        bytes_[lengthLocation] = (byte)((length >>> 8) & 0xff);
        bytes_[lengthLocation + 1] = (byte)(length & 0xff);
      }
  
      // now write the actual bytes
  
      if (netAgent_.typdef_.isCcsidMbcSet()) {
        write1Byte(0x00);
      }
      else {
        write1Byte(0xff);
        write1Byte(0x00);
        hasNullTermination = false;
      }
      write4Bytes(sqlLen);
      // need to split the packet with continuation headers if too large
      if (needsExtendedLength) {
        final int sizeWithoutDss = offset_ - dssLengthLocation_;
        int writeLen = DssConstants.MAX_DSS_LEN - sizeWithoutDss;
        writeBytes(sqlBytes, writeLen);
        sqlLen -= writeLen;
        while (sqlLen > DSS_CONTINUATION_LEN) {
          ensureLength(offset_ + DssConstants.MAX_DSS_LEN);
          bytes_[offset_++] = (byte)0xff;
          bytes_[offset_++] = (byte)0xff;
          System.arraycopy(sqlBytes, writeLen, bytes_, offset_,
              DSS_CONTINUATION_LEN);
          sqlLen -= DSS_CONTINUATION_LEN;
          writeLen += DSS_CONTINUATION_LEN;
          offset_ += DSS_CONTINUATION_LEN;
        }
        if (sqlLen > 0) {
          int leftLen = sqlLen + 2;
          ensureLength(offset_ + leftLen);
          bytes_[offset_++] = (byte)((leftLen >>> 8) & 0xff);
          bytes_[offset_++] = (byte)(leftLen & 0xff);
          System.arraycopy(sqlBytes, writeLen, bytes_, offset_, sqlLen);
          offset_ += sqlLen;
        }
      }
      else {
        writeBytes(sqlBytes, sqlLen);
      }
      if (hasNullTermination) {
        write1Byte(0xff);
      }
        /* (original code)
        if (string == null) {
            write2Bytes(0xffff);
        } else {
            byte[] sqlBytes = null;
            if (netAgent_.typdef_.isCcsidMbcSet()) {
                sqlBytes = getBytes(string, netAgent_.typdef_.getCcsidMbcEncoding());
                write1Byte(0x00);
                write4Bytes(sqlBytes.length);
                writeBytes(sqlBytes, sqlBytes.length);
                write1Byte(0xff);
            } else {
                sqlBytes = getBytes(string, netAgent_.typdef_.getCcsidSbcEncoding());
                write1Byte(0xff);
                write1Byte(0x00);
                write4Bytes(sqlBytes.length);
                writeBytes(sqlBytes, sqlBytes.length);
            }
        }
        */
// GemStone changes END
    }

    // SQLSTTGRP : FDOCA EARLY GROUP
    // SQL Statement Group Description
    //
    // FORMAT FOR SQLAM <= 6
    //   SQLSTATEMENT_m; PROTOCOL TYPE LVCM; ENVLID 0x40; Length Override 32767
    //   SQLSTATEMENT_s; PROTOCOL TYPE LVCS; ENVLID 0x34; Length Override 32767
    //
    // FORMAT FOR SQLAM >= 7
    //   SQLSTATEMENT_m; PROTOCOL TYPE NOCM; ENVLID 0xCF; Length Override 4
    //   SQLSTATEMENT_s; PROTOCOL TYPE NOCS; ENVLID 0xCB; Length Override 4
    private void buildSQLSTTGRP(String string) throws SqlException {
        buildNOCMorNOCS(string, true /* GemStoneAddition */);
        return;
    }

    // SQLSTT : FDOCA EARLY ROW
    // SQL Statement Row Description
    //
    // FORMAT FOR ALL SQLAM LEVELS
    //   SQLSTTGRP; GROUP LID 0x5C; ELEMENT TAKEN 0(all); REP FACTOR 1
    private void buildSQLSTT(String string) throws SqlException {
        buildSQLSTTGRP(string);
    }

    protected void buildSQLSTTcommandData(String sql) throws SqlException {
        createEncryptedCommandData();
        int loc = offset_;
        markLengthBytes(CodePoint.SQLSTT);
        buildSQLSTT(sql);
// GemStone changes BEGIN
        /* (original code)
        updateLengthBytes();
        */
// GemStone changes END
        if (netAgent_.netConnection_.getSecurityMechanism() ==
                NetConfiguration.SECMEC_EUSRIDDTA ||
                netAgent_.netConnection_.getSecurityMechanism() ==
                NetConfiguration.SECMEC_EUSRPWDDTA) {
            encryptDataStream(loc);
        }

    }


    protected void buildSQLATTRcommandData(String sql) throws SqlException {
        createEncryptedCommandData();
        int loc = offset_;
        markLengthBytes(CodePoint.SQLATTR);
        buildSQLSTT(sql);
// GemStone changes BEGIN
        /* (original code)
        updateLengthBytes();
        */
// GemStone changes END
        if (netAgent_.netConnection_.getSecurityMechanism() ==
                NetConfiguration.SECMEC_EUSRIDDTA ||
                netAgent_.netConnection_.getSecurityMechanism() ==
                NetConfiguration.SECMEC_EUSRPWDDTA) {
            encryptDataStream(loc);
        }

    }


    public void encryptDataStream(int lengthLocation) throws SqlException {
        byte[] clearedBytes = new byte[offset_ - lengthLocation];
        byte[] encryptedBytes;
        for (int i = lengthLocation; i < offset_; i++) {
            clearedBytes[i - lengthLocation] = bytes_[i];
        }

        encryptedBytes = netAgent_.netConnection_.getEncryptionManager().
                encryptData(clearedBytes,
                        NetConfiguration.SECMEC_EUSRIDPWD,
                        netAgent_.netConnection_.getTargetPublicKey(),
                        netAgent_.netConnection_.getTargetPublicKey());

        int length = encryptedBytes.length;

        if (bytes_.length >= lengthLocation + length) {
            System.arraycopy(encryptedBytes, 0, bytes_, lengthLocation, length);
        } else {
            byte[] largeByte = new byte[lengthLocation + length];
            System.arraycopy(bytes_, 0, largeByte, 0, lengthLocation);
            System.arraycopy(encryptedBytes, 0, largeByte, lengthLocation, length);
            bytes_ = largeByte;
        }

        offset_ += length - clearedBytes.length;

        //we need to update the length in DSS header here.

        bytes_[lengthLocation - 6] = (byte) ((length >>> 8) & 0xff);
        bytes_[lengthLocation - 5] = (byte) (length & 0xff);
    }

}
