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

package com.pivotal.gemfirexd.internal.engine.sql.execute;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.sql.compile.SQLMatcherConstants;
import com.pivotal.gemfirexd.internal.engine.sql.compile.Token;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.JDBC30Translation;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.ParameterValueSet;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.TypeCompiler;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecPreparedStatement;
import com.pivotal.gemfirexd.internal.iapi.types.DataTypeDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.SQLBoolean;

/**
 * ParameterValueSet to capture the constants in a statement as parameter so
 * that its transparently shipped to data nodes and executed there without
 * re-matching.
 * 
 * @author soubhikc
 * 
 */
public final class ConstantValueSetImpl implements ConstantValueSet {

  private static final String SINGLE_QUOTE = "\'";

  private transient DataValueDescriptor[] staticParameters;

  private DataTypeDescriptor[] paramTypes;
  private List<TypeCompiler> origParamTCs;
  private Activation activation;

  // utilizing it in serialization instead of creating fresh strings
  // [sumedh] this has the whole bunch of tokens via next ptr in Token
  // so create tokenKind/tokenImages arrays for both cases
  //private List<Token> constantTokenList;

  private int[] tokenKind;
  private String[] tokenImages;

  public ConstantValueSetImpl(
      Activation ownedactivation, List<Token> tokenlist,
      List<TypeCompiler> origParamTCs) {

    //paramTypes = parametertypes;
    activation = ownedactivation;
    final int constantSize = tokenlist.size();
    tokenKind = new int[constantSize];
    tokenImages = new String[constantSize];
    Token tok;
    for (int index = 0; index < constantSize; index++) {
      tok = tokenlist.get(index);
      tokenKind[index] = tok.kind;
      tokenImages[index] = tok.image;
    }
    this.origParamTCs = origParamTCs;

    //will create individual parameters lazily in getParameter(..)
    staticParameters = new DataValueDescriptor[constantSize];
  }

  public ConstantValueSetImpl(int[] tokentypes, String[] tokenstrings) {
    staticParameters = null;
    paramTypes = null;
    activation = null;
    tokenImages = tokenstrings;
    tokenKind = tokentypes;

    if (GemFireXDUtils.TraceStatementMatching) {
      SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_STATEMENT_MATCHING, "read "
          + this);
    }
  }

  public void setActivation(Activation ownedActivation) {
    activation = ownedActivation;
  }

  @Override
  public void initialize(DataTypeDescriptor[] types) throws StandardException {
    paramTypes = types;
    if(staticParameters == null) {
      staticParameters = new DataValueDescriptor[types.length];
    }
  }

  @Override
  public final String getConstantImage(int position) {

    final int kind = tokenKind[position];
    final String image = tokenImages[position];

    switch (kind) {
      case SQLMatcherConstants.STRING:
      case SQLMatcherConstants.HEX_STRING:
        return escapeQuotes(image);

      default:
        return image;
    }
  }

  public static final String tokenKindAsString(int kind) {

    switch (kind) {
      case SQLMatcherConstants.STRING:
        return "STRING";
      case SQLMatcherConstants.HEX_STRING:
        return "HEX_STRING";
      case SQLMatcherConstants.EXACT_NUMERIC:
        return "EXACT_NUMERIC";
      case SQLMatcherConstants.EOF:
        // TODO move this to default once all data types are handled.
        SanityManager.THROWASSERT("ConstantValueSet: Invalid token received "
            + kind);
      default:
        return String.valueOf(kind);
    }
  }

  private String escapeQuotes(String image) {
    int index = image.indexOf(SINGLE_QUOTE, 0);

    while (index != -1 && index < image.length()) {
      image = image.substring(0, index+1) + SINGLE_QUOTE
          + image.substring(index + 1);
      index = image.indexOf(SINGLE_QUOTE, index + 2);
    }

    return SINGLE_QUOTE + image + SINGLE_QUOTE;
  }

  @Override
  public final DataValueDescriptor getParameter(int position)
      throws StandardException {
    if (staticParameters == null) {
      if (SanityManager.DEBUG) {
        final ExecPreparedStatement ps = activation.getPreparedStatement();
        SanityManager.THROWASSERT(GemFireXDUtils.addressOf(this)
            + ":getParameter(int): ["
            + (ps != null ? ps.getStatement() : " NULL ")
            + "] StaticParameterSet is found null but parser have "
            + "identified the constants.");
      }
      throw StandardException.newException(SQLState.NO_INPUT_PARAMETERS);
    }

    if (staticParameters[position] == null) {
      final DataValueDescriptor dvd = paramTypes[position].getNull();
   /*   if (this.origParamTypes != null) {

        final DataValueDescriptor dvdOriginal = origParamTypes[position]
            .getNull();
        if (constantTokenList == null) {
          assert tokenImages != null;
          dvdOriginal.setValue(tokenImages[position]);
          // dvd.setValue(tokenImages[position]);
          dvd.normalize(paramTypes[position], dvdOriginal);
        }
        else {
          dvdOriginal.setValue(constantTokenList.get(position).image);
          dvd.normalize(paramTypes[position], dvdOriginal);
        }
      }
      else {*/

        dvd.setValue(tokenImages[position]);
     // }

      staticParameters[position] = dvd;
    }

    if (SanityManager.DEBUG) {
      final ExecPreparedStatement ps = activation.getPreparedStatement();
      if (GemFireXDUtils.TraceStatementMatching && SanityManager.isFinerEnabled) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_STATEMENT_MATCHING,
            GemFireXDUtils.addressOf(this) + ":getParameter(int): ["
                + (ps != null ? ps.getStatement() : " NULL ")
                + "] returning  staticParams[" + position + "]="
                + staticParameters[position]);
      }
    }

    return staticParameters[position];
  }

  @Override
  public int getParameterCount() {
    return (staticParameters != null ? staticParameters.length
        : tokenImages != null ? tokenImages.length : 0);
  }

  /**
   * This asserts certain expectations on data nodes when constants are
   * propagated via StmtExecutorMessage.
   */
  @Override
  public void validate() throws StandardException {

    if (tokenImages == null || tokenImages.length <= 0) {
      SanityManager
          .THROWASSERT("ConstantValueSet: Token images are supposed to be existent");
    }
    if (tokenKind == null || tokenKind.length <= 0) {
      SanityManager
          .THROWASSERT("ConstantValueSet: Token kind should be non-null");
    }

    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TraceStatementMatching) {
        StringBuilder sb = new StringBuilder(
            "ConstantValueSet: Validating tokens : [");

        for (int i = 0; i < tokenImages.length; i++) {
          if (i != 0)
            sb.append(",");
          sb.append(tokenImages[i]).append(" (")
              .append(tokenKindAsString(tokenKind[i])).append(")");
        }
        sb.append("]");
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_STATEMENT_MATCHING,
            sb.toString());
      }
    }
  }

  @Override
  public int validateParameterizedData() throws StandardException {
    assert (paramTypes == null && tokenImages == null)
    || paramTypes.length == tokenImages.length:
      "ConstantValueSet: parameters type definition should be "
        + "equal to number of tokens";
    
    if (SanityManager.DEBUG) {
      int constantSize = 0;
      if(this.tokenImages != null) {
        constantSize = this.tokenImages.length;
      }
      if (constantSize != (paramTypes != null ? paramTypes.length : 0)) {
        SanityManager.THROWASSERT("constantTokenList=" + constantSize
            + " typeDescriptors="
            + (paramTypes != null ? paramTypes.length : 0));
      }
    }
    int paramCount = getParameterCount();
    for (int i = 0; i < paramCount; ++i) {
      if (!this.origParamTCs.get(i).compatible(this.paramTypes[i].getTypeId())) {
        if (this.paramTypes[i].getTypeId().isBooleanTypeId()) {
          // check if the token can be stored in a boolean in which case it is
          // compatible
          SQLBoolean.getBoolean(tokenImages[i]);
        }
        else {
          return i;
        }
      }
    }
    return -1;
  }

  @Override
  public void transferDataValues(ParameterValueSet pvstarget)
      throws StandardException {

    if (SanityManager.DEBUG) {
      if (!getClass().isInstance(pvstarget)) {
        SanityManager.THROWASSERT("Incapable of transfering parameters to "
            + pvstarget.getClass().getName());
      }
    }

    ConstantValueSetImpl target = (ConstantValueSetImpl)pvstarget;
    target.staticParameters = new DataValueDescriptor[staticParameters.length];
    target.paramTypes = paramTypes;
    target.tokenKind = tokenKind;
    target.tokenImages = tokenImages;
  }

  public void setTransferData(DataTypeDescriptor[] paramTypes, int[] tokenKind,
      String[] tokenImages) {
    this.paramTypes = paramTypes;
    this.tokenKind = tokenKind;
    this.tokenImages = tokenImages;
    this.staticParameters = new DataValueDescriptor[paramTypes.length];
  }

  public static void writeBytes(DataOutput out, ParameterValueSet pvs)
      throws IOException {
    if (pvs == null || pvs.getParameterCount() <= 0) {
      InternalDataSerializer.writeArrayLength(-1, out);
      return;
    }

    assert pvs instanceof ConstantValueSet:
      "ConstantValueSet: pvs must be an instanceof this class.";

    final ConstantValueSet cvs = (ConstantValueSet)pvs;
    final int paramCount = cvs.getParameterCount();
    //DataTypeDescriptor [] paramTypes = cvs.getParamTypes();
    //List<Token> constantTokenList = cvs.getTokenList();
    //assert paramTypes.length == paramCount :
    //"ConstantValueSet: param length should be equal to paramCount determined.";

    InternalDataSerializer.writeArrayLength(paramCount, out);

    final int[] tokenKinds = cvs.getTokenKinds();
    final String[] tokenImages = cvs.getTokenImages();
    for (int index = 0; index < paramCount; index++) {
      InternalDataSerializer.writeSignedVL(tokenKinds[index], out);
      InternalDataSerializer.writeString(tokenImages[index], out);
    }

    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TraceStatementMatching) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_STATEMENT_MATCHING,
            "ConstantValueSet: written " + Arrays.toString(tokenImages));
      }
    }
  }

  public static ConstantValueSetImpl readBytes(DataInput in) throws IOException {
    int paramCount = InternalDataSerializer.readArrayLength(in);
    if (paramCount <= 0) {
      return null;
    }

    final int[] paramkind = new int[paramCount];
    final String[] paramstrings = new String[paramCount];
    for (int i = 0; i < paramCount; i++) {
      paramkind[i] = (int)InternalDataSerializer.readSignedVL(in);
      paramstrings[i] = InternalDataSerializer.readString(in);
    }

    return new ConstantValueSetImpl(paramkind, paramstrings);
  }

  public void refreshTypes(DataTypeDescriptor[] constantDescriptors) {
    paramTypes = constantDescriptors;
    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TraceStatementMatching) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_STATEMENT_MATCHING, this
            + " received parameter descriptors " + Arrays.toString(paramTypes));
      }
    }
    assert tokenImages.length == paramTypes.length;
    staticParameters = new DataValueDescriptor[paramTypes.length];
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("ConstantValueSet@");
    sb.append(Integer.toHexString(System.identityHashCode(this)));

    if (tokenImages != null) {
      sb.append(" tokens: " );
      boolean firstCall = true;
      for (String s : tokenImages) {
        if (firstCall) {
          firstCall = false;
        }
        else {
          sb.append(',');
        }
        sb.append(s != null ? s : "NULL");
      }
    }
    else {
      sb.append(" NONE ");
    }
    return sb.toString();
  }

  @Override
  public int allAreSet() {
    // we are all set once object created.
    return 0;
  }

  @Override
  public boolean checkNoDeclaredOutputParameters() {
    // no question of OUT parameters.
    return false;
  }

  @Override
  public void clearParameters() {
    throw new IllegalStateException("This should not have got invoked as "
        + "batch operation is not possible for statements");
  }

  @Override
  public ParameterValueSet getClone() {
    throw new IllegalStateException("This should not have got invoked as "
        + "we don't want the shell object to be cloned ever");
  }

  @Override
  public DataValueDescriptor getParameterForGet(int position)
      throws StandardException {
    throw new IllegalStateException(
        "This should not have got invoked as everything is only incoming");
  }

  @Override
  public DataValueDescriptor getParameterForSet(int position)
      throws StandardException {
    throw new IllegalStateException(
        "This should not have got invoked as everything is only incoming");
  }

  @Override
  public short getParameterMode(int parameterIndex) {
    return JDBC30Translation.PARAMETER_MODE_IN;
  }

  @Override
  public int getPrecision(int parameterIndex) {
    throw new IllegalStateException(
        "This should not have got invoked as outgoing parameter not cater for");
  }

  @Override
  public DataValueDescriptor getReturnValueForSet() throws StandardException {
    throw new IllegalStateException(
        "This should not have got invoked as outgoing parameter not cater for");
  }

  @Override
  public int getScale(int parameterIndex) {
    throw new IllegalStateException(
        "This should not have got invoked as outgoing parameter not cater for");
  }

  @Override
  public boolean hasReturnOutputParameter() {
    return false;
  }

  @Override
  public void registerOutParameter(int parameterIndex, int sqlType, int scale)
      throws StandardException {
    throw new IllegalStateException(
        "This should not have got invoked as outgoing parameter not cater for");
  }

  @Override
  public void setParameterAsObject(int parameterIndex, Object value)
      throws StandardException {

  }

  @Override
  public void setParameterMode(int position, int mode) {
    throw new IllegalStateException(
        "This should not have got invoked only PARAMETER_MODE_IN is expected");
  }

  @Override
  public boolean isListOfConstants() {
    return true;
  }

  @Override
  public boolean canReleaseOnClose() {
    return false;
  }

  @Override
  public String[] getTokenImages() {
    return this.tokenImages;
  }

  @Override
  public int[] getTokenKinds() {
    return this.tokenKind;
  }

  @Override
  public DataTypeDescriptor[] getParamTypes() {
    return this.paramTypes;
  }

  @Override
  public List<TypeCompiler> getOrigTypeCompilers() {
    return this.origParamTCs;
  }
}
