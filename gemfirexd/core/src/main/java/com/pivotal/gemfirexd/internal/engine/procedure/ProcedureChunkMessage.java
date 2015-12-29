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

package com.pivotal.gemfirexd.internal.engine.procedure;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import com.gemstone.gemfire.DataSerializer;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserver;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.engine.GfxdDataSerializable;

public final class ProcedureChunkMessage extends GfxdDataSerializable implements
    Serializable {

  private static final long serialVersionUID = -1977996002198903592L;

  public final static byte UNKNOWN_TYPE = 0;

  public final static byte OUT_PARAMETER = 1;

  public final static byte META_DATA = 2;

  public final static byte RESULTSET = 3;

  public final static byte PROCEDURE_END = 4;

  public final static String[] typeName = { "Unknown type", "Out parameter",
      "Meta data", "Resultset", "Procedure end" };

  private byte type;

  private int resultSetNumber;

  private boolean lastChunk;

  // these varaibles are used by receiving side to process the messages from a
  // data node
  // in FIFO. The purpose of the prevSeqNumber is used to simplify the
  // implementation so
  // that we do not need to consider the overflow of the seqNumber.
  private int seqNumber;

  private int prevSeqNumber;

  transient private int colNumber; // how many columns for the result set

  transient private int rowNumber; // how many rows in this message;

  transient private ArrayList<List<Object>> chunks;

  public ProcedureChunkMessage() {
    this.chunks = null;
    this.colNumber = 0;
    this.resultSetNumber = -1;
    this.rowNumber = 0;
    this.type = UNKNOWN_TYPE;
    this.lastChunk = false;
  }

  public ProcedureChunkMessage(byte type) {
    this.chunks = null;
    this.colNumber = 0;
    this.resultSetNumber = -1;
    this.rowNumber = 0;
    this.type = type;
    this.lastChunk = false;
  }

  public ProcedureChunkMessage(byte type, int resultSetNumber,
      ArrayList<List<Object>> chunks) {
    this.type = type;
    this.resultSetNumber = resultSetNumber;
    this.chunks = chunks;
    this.lastChunk = false;
    if (this.chunks == null || this.chunks.size() == 0) {
      this.colNumber = 0;
      this.rowNumber = 0;
    }
    else {
      this.rowNumber = this.chunks.size();
      this.colNumber = this.chunks.get(0).size();
    }
  }

  public final int getSeqNumber() {
    return seqNumber;
  }

  public final void setSeqNumber(final int seqNumber) {
    this.seqNumber = seqNumber;
  }

  public final int getPrevSeqNumber() {
    return prevSeqNumber;
  }

  public final void setPrevSeqNumber(final int prevSeqNumber) {
    this.prevSeqNumber = prevSeqNumber;
  }

  /***
   * 
   * @return
   */
  public byte getType() {
    return type;
  }

  /***
   * 
   * @param type
   */
  public void setType(byte type) {
    this.type = type;
  }

  /***
   * 
   * @return
   */
  public int getColumnNumber() {
    return colNumber;
  }

  /***
   * 
   * @return
   */
  public int getRowNumber() {
    return rowNumber;
  }

  /***
   * 
   * @return
   */
  public ArrayList<List<Object>> getChunks() {
    return chunks;
  }

  /***
   * 
   * @param chunks
   */
  public void setChunks(ArrayList<List<Object>> chunks) {
    this.chunks = chunks;
  }

  /**
   * 
   * @return
   */
  public int getResultSetNumber() {
    return resultSetNumber;
  }

  /**
   * 
   * @param resultSetNumber
   */
  public void setResultSetNumber(int resultSetNumber) {
    this.resultSetNumber = resultSetNumber;
  }

  @Override
  public void fromData(DataInput in) throws IOException,
          ClassNotFoundException {
    super.fromData(in);
    this.type = in.readByte();
    this.resultSetNumber = in.readInt();
    this.rowNumber = in.readInt();
    this.colNumber = in.readInt();
    this.lastChunk = in.readBoolean();
    this.seqNumber = in.readInt();
    this.prevSeqNumber = in.readInt();
    this.chunks = new ArrayList<List<Object>>(); // rowNumber][colNumber];

    for (int rowIndex = 0; rowIndex < this.rowNumber; ++rowIndex) {
      List<Object> row = new ArrayList<Object>();
      for (int colIndex = 0; colIndex < this.colNumber; ++colIndex) {
        row.add(DataSerializer.readObject(in));
      }
      this.chunks.add(row);
    }
  }

  @Override
  public void toData(final DataOutput out) throws IOException {
    super.toData(out);
    final GemFireXDQueryObserver observer = GemFireXDQueryObserverHolder
        .getInstance();
    if (observer != null) {
      observer.beforeProcedureChunkMessageSend(this);
    }
    out.writeByte(this.type);
    out.writeInt(this.resultSetNumber);
    out.writeInt(this.rowNumber);
    out.writeInt(this.colNumber);
    out.writeBoolean(this.lastChunk);
    out.writeInt(this.seqNumber);
    out.writeInt(this.prevSeqNumber);
    for (int rowIndex = 0; rowIndex < this.rowNumber; ++rowIndex) {
      List<Object> row = this.chunks.get(rowIndex);
      for (int colIndex = 0; colIndex < this.colNumber; ++colIndex) {
        DataSerializer.writeObject(row.get(colIndex), out);
      }
    }
  }

  public void setLast() {
    this.lastChunk = true;
  }

  public boolean isLast() {
    return this.lastChunk;
  }

  @Override
  public String toString() {
    final StringBuilder str = new StringBuilder();
    str.append("ResultSetID=").append(this.resultSetNumber);
    str.append(";Type=").append(typeName[this.type]);
    str.append(";PrevSeq=").append(this.prevSeqNumber);
    str.append(";CurrentSeq=").append(this.seqNumber);

    if (this.chunks != null) {
      int rowIndex = 0;
      for (List<Object> row : this.chunks) {
        str.append(" row#" + rowIndex);
        for (Object o : row) {
          str.append(" " + o);
        }
        ++rowIndex;
      }
    }
    return str.toString();
  }

  @Override
  public byte getGfxdID() {
    return PROCEDURE_CHUNK_MSG;
  }
}
