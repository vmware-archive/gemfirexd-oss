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

package com.pivotal.gemfirexd.internal.impl.sql.compile;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.gemstone.gnu.trove.THashSet;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.CostEstimate;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.Optimizable;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.ConglomerateDescriptor;
import com.pivotal.gemfirexd.internal.iapi.store.access.Qualifier;
import com.pivotal.gemfirexd.internal.iapi.util.JBitSet;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;

/**
 * This class is for generating an optimized plan for a list of ORs using a
 * MultiColumnTableScanResultSet when indexes can be used for each of the ORs.
 * 
 * This works only if all the ORs refer to the same table/view.
 */
public final class OrListNode extends AndNode {

  protected int tableNumber;

  protected THashSet columns;

  protected ValueNodeList vnl;

  static final class ElementSpecification {

    final ConglomerateDescriptor conglom;
    final PredicateList predList;
    final CostEstimate costEstimate;
    final int bulkFetch;
    final boolean multiProbing;

    ElementSpecification(final ConglomerateDescriptor conglom,
        final PredicateList predList, final CostEstimate costEstimate,
        final int bulkFetch, final boolean multiProbing) {
      this.conglom = conglom;
      this.predList = predList;
      this.costEstimate = costEstimate;
      this.bulkFetch = bulkFetch;
      this.multiProbing = multiProbing;
    }
  }

  protected transient ArrayList<ElementSpecification> elements;

  /**
   * Initializer for an OrListNode
   * 
   * @param operandList
   *          The list of ORs
   */
  @Override
  public void init(Object tableNum, Object cols, Object operandList) {
    this.tableNumber = (Integer) tableNum;
    this.columns = (THashSet) cols;
    this.vnl = (ValueNodeList) operandList;
    // masquerade as an AndNode at the rightmost branch in the tree
    // we will optimize and generate code as required later
    int size = this.vnl.size();
    OrNode lastNode = (OrNode) this.vnl.elementAt(size - 1);
    ValueNode ln = lastNode.getLeftOperand();
    ValueNode rn = lastNode.getRightOperand();
    while (ln instanceof AndNode) {
      AndNode an = (AndNode) ln;
      ln = an.getLeftOperand();
      rn = an.getRightOperand();
    }
    while (rn instanceof AndNode) {
      rn = ((AndNode) rn).getRightOperand();
    }
    super.init(ln, rn);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ValueNode preprocess(int numTables, FromList outerFromList,
      SubqueryList outerSubqueryList, PredicateList outerPredicateList)
      throws StandardException {
    if (getCompilerContext().orListOptimizationAllowed()) {
      return this;
    } else {
      return getOrNode(0).preprocess(numTables, outerFromList,
          outerSubqueryList, outerPredicateList);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ValueNode changeToCNF(boolean underTopAndNode)
      throws StandardException {
    return this;
  }

  public final void addElement(ConglomerateDescriptor bestConglom,
      PredicateList predList, CostEstimate bestCost, int bulkFetch,
      boolean multiProbing) {
    if (this.elements == null) {
      this.elements = new ArrayList<ElementSpecification>();
    }
    this.elements.add(new ElementSpecification(bestConglom, predList, bestCost,
        bulkFetch, multiProbing));
  }

  public final boolean hasOptimizedList() {
    return this.elements != null && this.elements.size() > 0;
  }

  final List<ElementSpecification> getElements() {
    return this.elements;
  }

  /**
   * Get the table number for which this OR list has been created.
   */
  public final int getTableNumber() {
    return this.tableNumber;
  }

  /**
   * Get the OR node in this list at the given zero based index.
   */
  public final OrNode getOrNode(int index) {
    return (OrNode) this.vnl.elementAt(index);
  }

  /**
   * Get the ValueNodes for this node.
   */
  public final ValueNodeList getValueNodes() {
    return this.vnl;
  }

  /**
   * Get the set of columns referenced in this OR list.
   */
  public final THashSet getColumns() {
    return this.columns;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getTableName() {
    return getOrNode(0).getTableName();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getSchemaName() {
    return getOrNode(0).getSchemaName();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String findSourceSchemaName() {
    return getOrNode(0).findSourceSchemaName();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String findSourceTableName() {
    return getOrNode(0).findSourceTableName();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public double selectivity(Optimizable optTable) throws StandardException {
    double sel = 0.0;
    for (QueryTreeNode vn : this.vnl) {
      double v = ((ValueNode) vn).selectivity(optTable);
      if (v > sel) {
        sel = v;
      }
    }
    return sel;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getLeftOperatorForCompare() {
    // send back a non-equality operator so that no elimination happens
    // due to this list
    return RelationalOperator.LESS_THAN_RELOP;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected int getOrderableVariantType() throws StandardException {
    int variantType = Qualifier.CONSTANT;
    int vt;
    final int size = this.vnl.size();
    for (int index = 0; index < size; index++) {
      final OrNode on = (OrNode) this.vnl.elementAt(index);
      // last element will not have OR on its right
      ValueNode vn = index != (size - 1) ? on.getLeftOperand() : on;
      if (variantType > (vt = vn.getOrderableVariantType())) {
        variantType = vt;
      }
    }
    return variantType;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected boolean isEquivalent(ValueNode other) throws StandardException {
    if (isSameNodeType(other)) {
      final ValueNodeList ovnl = ((OrListNode) other).vnl;
      final int size = this.vnl.size();
      if (size == ovnl.size()) {
        for (int index = 0; index < size; index++) {
          final OrNode on = (OrNode) this.vnl.elementAt(index);
          final OrNode on2 = (OrNode) ovnl.elementAt(index);
          // last element will not have OR on its right
          ValueNode vn, vn2;
          if (index != (size - 1)) {
            vn = on.getLeftOperand();
            vn2 = on2.getLeftOperand();
          } else {
            vn = on;
            vn2 = on2;
          }
          if (!vn.isEquivalent(vn2)) {
            return false;
          }
        }
        return true;
      }
    }
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean categorize(JBitSet referencedTabs, boolean simplePredsOnly)
      throws StandardException {
    // the first OR itself will do the categorization since it is connected
    // to all other OR nodes via the rightOperand
    return getOrNode(0).categorize(referencedTabs, simplePredsOnly);
  }

  /**
   * Convert this object to a String. See comments in QueryTreeNode.java for how
   * this should be done for tree printing.
   * 
   * @return This object as a String
   */
  @Override
  public String toString() {
    if (SanityManager.DEBUG) {
      StringBuilder sb = new StringBuilder();
      for (QueryTreeNode qtn : this.vnl) {
        sb.append("ValueNode: ").append(qtn.toString());
      }
      return sb.toString();
    } else {
      return "";
    }
  }

  public ColumnReference getColumnOperand(Optimizable optTable,
      int columnPosition) {


    FromTable ft = (FromTable) optTable;
    // When searching for a matching column operand, we search
    // the entire subtree (if there is one) beneath optTable
    // to see if we can find any FromTables that correspond to
    // either of this op's column references.
    Iterator<?> colRefIter = this.columns.iterator();
    while (colRefIter.hasNext()) {
   ColumnReference cr = (ColumnReference)colRefIter.next();
      if (((BinaryRelationalOperatorNode)this.leftOperand)
          .valNodeReferencesOptTable(cr, ft, false, true)) {
        if (cr.getSource().getColumnPosition() == columnPosition) {
          /* We've found the correct column - return it */
          return cr;
        }
      }
    }
    return null;

  }


}
