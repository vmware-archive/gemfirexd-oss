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
package com.pivotal.gemfirexd.internal.engine.distributed;

import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.ByteArrayDataInput;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.store.RowFormatter;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.ParameterValueSet;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;

/**
 * This is a utility class which writes the DVD[] in an optimized 
 * manner by using bitmask for null fields  so that a bit represents
 * a column's null/not null nature.
 * The main benefit of this type of IO is that 
 * <p>
 * 1) A null field is represented by a bit rather than a byte
 * <p>
 * 2) The SqlType would not be written as it is assumed that the
 *  DataValueDescriptor template  will be available on the reader.
 *  So the IO writes/read the undrlying data contained in the 
 *  DataValueDescriptor rather than  the DataValueDescriptor 
 *  itself.
 *  <p>
 *  This is utilized by the {@link ResultHolder} and  {@link PrepStatementQueryExecutorFunction}
 * @author Asif
 * @since Sql Fabric
 *
 */
public class DVDIOUtil
{
  static {
    ByteArrayDataOutput.init();
  }

  /**
   * Populates the DVD array passed by reading from the DataInputStream.
   * The stream contains  a byte or a byte array acting as bit mask for the
   * DVDs which is followed by the data for the DVDs . 
   * @param dvds Array of DataValueDescriptor which needs to be populated
   * @param dis DataInputStream which contains the bitmask for the fields followed by the data contained in the DataValueDescriptor object.
   * @param numEightColGrps  Number of eight column groups needed
   * @param numPartialCols Number of columns present in the last group
   * @throws IOException
   * @throws ClassNotFoundException
   */
  public static void readDVDArray(DataValueDescriptor[] dvds,
      final ByteArrayDataInput dis, final int numEightColGrps,
      final int numPartialCols) throws IOException, ClassNotFoundException {
    int groupNum = 0;
    for(;groupNum <numEightColGrps-1;++groupNum) {        
      readAGroup(groupNum,8, dvds,dis) ;
    }
    readAGroup(groupNum, numPartialCols, dvds, dis); 
    
  }

  public static void writeDVDArray(DataValueDescriptor[] dvdArr,
      final int numEightColGroups, final int numPartialCols, DataOutput dos)
      throws IOException, StandardException {

    if (GemFireXDUtils.TraceQuery | GemFireXDUtils.TraceNCJ) {
      final StringBuilder sb = new StringBuilder(
          "DVDIOUtil::writeDVDArray: writing dvd array DVD array { ");
      if (dvdArr.length > 0) {
        DataValueDescriptor dvd;
        for (int index = 0; index < dvdArr.length; ++index) {
          if (index > 0) {
            sb.append(", ");
          }
          dvd = dvdArr[index];
          if (dvd != null) {
            if (RowFormatter.isLob(dvd.getTypeFormatId())) {
              sb.append('(').append(dvd.getTypeName()).append(";length=")
                  .append(dvd.getLength()).append(";hashCode=0x")
                  .append(Integer.toHexString(dvd.hashCode())).append(')');
            }
            else {
              sb.append(dvd.toString());
            }
          }
          else {
            sb.append("(NULL)");
          }
        }
      }
      sb.append(" }");

      SanityManager
          .DEBUG_PRINT(GfxdConstants.TRACE_QUERYDISTRIB, sb.toString());
    }
    int groupNum = 0;
    for (; groupNum < numEightColGroups - 1; ++groupNum) {
      writeAGroup(groupNum, 8, dvdArr, dos);
    }
    writeAGroup(groupNum, numPartialCols, dvdArr, dos);
  }

  public static void writeParameterValueSet(ParameterValueSet pvs,
      final int numEightColGroups, final int numPartialCols, DataOutput dos)
      throws IOException, StandardException {

    if (GemFireXDUtils.TraceQuery | GemFireXDUtils.TraceNCJ) {
      StringBuilder paramStr = new StringBuilder("DVDIOUtil#"
          + "writeParameterValueSet: writing parameter array with values [");

      for (int index = 0; index < pvs.getParameterCount(); ++index) {
        DataValueDescriptor dvd = pvs.getParameter(index);
        if (index > 0) {
          paramStr.append(", ");
        }
        if (dvd != null) {
          if (RowFormatter.isLob(dvd.getTypeFormatId())) {
            paramStr.append('(').append(dvd.getTypeName()).append(";length=")
                .append(dvd.getLength()).append(";hashCode=0x")
                .append(Integer.toHexString(dvd.hashCode())).append(')');
          }
          else {
            paramStr.append(dvd.toString());
          }
        }
        else {
          paramStr.append("(NULL)");
        }
      }
      paramStr.append(']');
      SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_QUERYDISTRIB,
          paramStr.toString());
    }
    int groupNum = 0;
    for (; groupNum < numEightColGroups; ++groupNum) {
      writeAGroup(groupNum, 8, pvs, dos);
    }
    if (numPartialCols > 0) {
      writeAGroup(groupNum, numPartialCols, pvs, dos);
    }
  }

  public static void readParameterValueSet(ParameterValueSet pvs,
      final ByteArrayDataInput dis, final int numEightColGrps,
      final int numPartialCols) throws IOException, ClassNotFoundException,
      StandardException {
    int groupNum = 0;
    for (; groupNum < numEightColGrps; ++groupNum) {
      readAGroup(groupNum, 8, pvs, dis);
    }
    if (numPartialCols > 0) {
      readAGroup(groupNum, numPartialCols, pvs, dis);
    }
  }

  private static void writeAGroup(final int groupNum, final int numColsInGrp,
      final DataValueDescriptor[] dvdArr, DataOutput dos) throws IOException {
    byte activeByteForGroup = 0x00;
    int dvdIndex;
    for (int index = 0; index < numColsInGrp; ++index) {
      dvdIndex = (groupNum << 3) + index;
      if (!dvdArr[dvdIndex].isNull()) {
        activeByteForGroup = ActiveColumnBits
            .setFlagForNormalizedColumnPosition(index, activeByteForGroup);
      }
    }
    DataSerializer.writePrimitiveByte(activeByteForGroup, dos);
    if (GemFireXDUtils.TraceRSIter) {
      SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_RSITER,
          "DVDIOUtil::writeAGroup: activeByteForGroup=" + activeByteForGroup);
    }
    for (int index = 0; index < numColsInGrp; ++index) {
      dvdIndex = (groupNum << 3) + index;
      if (ActiveColumnBits.isNormalizedColumnOn(index, activeByteForGroup)) {
        if (GemFireXDUtils.TraceRSIter) {
          SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_RSITER,
              "DVDIOUtil::writeAGroup: writing DVD of type "
                  + dvdArr[dvdIndex].getTypeName() + ", class: "
                  + dvdArr[dvdIndex].getClass().getName());
        }
        dvdArr[dvdIndex].toDataForOptimizedResultHolder(dos);
      }
      else {
        if (GemFireXDUtils.TraceRSIter) {
          SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_RSITER,
              "DVDIOUtil::writeAGroup: skipping null DVD of type "
                  + dvdArr[dvdIndex].getTypeName() + ", class: "
                  + dvdArr[dvdIndex].getClass().getName());
        }
      }
    }
  }

  private static void writeAGroup(final int groupNum, final int numColsInGrp,
      final ParameterValueSet pvs, DataOutput dos) throws IOException,
      StandardException {
    byte activeByteForGroup = 0x00;
    int dvdIndex;
    for (int index = 0; index < numColsInGrp; ++index) {
      dvdIndex = (groupNum << 3) + index;
      if (!pvs.getParameter(dvdIndex).isNull()) {
        activeByteForGroup = ActiveColumnBits
            .setFlagForNormalizedColumnPosition(index, activeByteForGroup);
      }
    }
    DataSerializer.writePrimitiveByte(activeByteForGroup, dos);
    if (GemFireXDUtils.TraceRSIter) {
      SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_RSITER,
          "DVDIOUtil::writeAGroup: activeByteForGroup=" + activeByteForGroup);
    }
    for (int index = 0; index < numColsInGrp; ++index) {
      dvdIndex = (groupNum << 3) + index;
      if (ActiveColumnBits.isNormalizedColumnOn(index, activeByteForGroup)) {
        if (GemFireXDUtils.TraceRSIter) {
          SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_RSITER,
              "DVDIOUtil::writeAGroup: writing DVD of type "
                  + pvs.getParameter(dvdIndex).getTypeName() + ", class: "
                  + pvs.getParameter(dvdIndex).getClass().getName());
        }
        pvs.getParameter(dvdIndex).toDataForOptimizedResultHolder(dos);
      }
      else {
        if (GemFireXDUtils.TraceRSIter) {
          SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_RSITER,
              "DVDIOUtil::writeAGroup: skipping null DVD of type "
                  + pvs.getParameter(dvdIndex).getTypeName() + ", class: "
                  + pvs.getParameter(dvdIndex).getClass().getName());
        }
      }
    }
  }

  private static void readAGroup(final int groupNum, final int numColsInGrp,
      final DataValueDescriptor[] dvds, final ByteArrayDataInput dis)
      throws IOException, ClassNotFoundException {
    byte activeByteForGroup = DataSerializer.readPrimitiveByte(dis);
    if (GemFireXDUtils.TraceRSIter) {
      SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_RSITER,
          "DVDIOUtil::readAGroup: activeByteForGroup=" + activeByteForGroup);
    }
    int dvdIndex;
    for (int index = 0; index < numColsInGrp; ++index) {
      dvdIndex = (groupNum << 3) + index;
      if (ActiveColumnBits.isNormalizedColumnOn(index, activeByteForGroup)) {
        if (GemFireXDUtils.TraceRSIter) {
          SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_RSITER,
              "DVDIOUtil::readAGroup: reading DVD of type "
                  + dvds[dvdIndex].getTypeName() + ", class: "
                  + dvds[dvdIndex].getClass().getName());
        }
        dvds[dvdIndex].fromDataForOptimizedResultHolder(dis);
      }
      else {
        if (GemFireXDUtils.TraceRSIter) {
          SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_RSITER,
              "DVDIOUtil::readAGroup: skipping null DVD of type "
                  + dvds[dvdIndex].getTypeName() + ", class: "
                  + dvds[dvdIndex].getClass().getName());
        }
        dvds[dvdIndex].setToNull();
      }
    }
  }

  private static void readAGroup(final int groupNum, final int numColsInGrp,
      final ParameterValueSet pvs, final ByteArrayDataInput dis)
      throws IOException, ClassNotFoundException, StandardException {
    byte activeByteForGroup = DataSerializer.readPrimitiveByte(dis);
    int pvsIndex;
    DataValueDescriptor dvd;
    for (int index = 0; index < numColsInGrp; ++index) {
      pvsIndex = (groupNum << 3) + index;
      dvd = pvs.getParameterForSet(pvsIndex);
      if (ActiveColumnBits.isNormalizedColumnOn(index, activeByteForGroup)) {
        if (GemFireXDUtils.TraceRSIter) {
          SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_RSITER,
              "DVDIOUtil::readAGroup: reading DVD of type " + dvd.getTypeName()
                  + ", class: " + dvd.getClass().getName());
        }
        dvd.fromDataForOptimizedResultHolder(dis);
      }
      else {
        if (GemFireXDUtils.TraceRSIter) {
          SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_RSITER,
              "DVDIOUtil::readAGroup: skipping null DVD of type "
                  + dvd.getTypeName() + ", class: " + dvd.getClass().getName());
        }
        dvd.setToNull();
      }
    }
  }
}
