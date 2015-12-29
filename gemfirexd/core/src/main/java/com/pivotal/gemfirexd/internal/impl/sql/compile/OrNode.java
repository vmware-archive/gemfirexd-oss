/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.compile.OrNode

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

package	com.pivotal.gemfirexd.internal.impl.sql.compile;


import com.gemstone.gnu.trove.THashSet;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.C_NodeTypes;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.CompilerContext;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.Optimizable;
import com.pivotal.gemfirexd.internal.iapi.util.JBitSet;

import java.util.ArrayDeque;
import java.util.Vector;

public class OrNode extends BinaryLogicalOperatorNode
{
	/* Is this the 1st OR in the OR chain? */
	private boolean firstOr;

	/**
	 * Initializer for an OrNode
	 *
	 * @param leftOperand	The left operand of the OR
	 * @param rightOperand	The right operand of the OR
	 */

	public void init(Object leftOperand, Object rightOperand)
	{
		super.init(leftOperand, rightOperand, "or");
		this.shortCircuitValue = true;
	}

	/**
	 * Mark this OrNode as the 1st OR in the OR chain.
	 * We will consider converting the chain to an IN list
	 * during preprocess() if all entries are of the form:
	 *		ColumnReference = expression
	 */
	void setFirstOr()
	{
		firstOr = true;
	}

	/**
	 * Bind this logical operator.  All that has to be done for binding
	 * a logical operator is to bind the operands, check that both operands
	 * are BooleanDataValue, and set the result type to BooleanDataValue.
	 *
	 * @param fromList			The query's FROM list
	 * @param subqueryList		The subquery list being built as we find SubqueryNodes
	 * @param aggregateVector	The aggregate vector being built as we find AggregateNodes
	 *
	 * @return	The new top of the expression tree.
	 *
	 * @exception StandardException		Thrown on error
	 */

	public ValueNode bindExpression(
		FromList fromList, SubqueryList subqueryList,
		Vector	aggregateVector)
			throws StandardException
	{
		super.bindExpression(fromList, subqueryList, aggregateVector);
		postBindFixup();
		return this;
	}
	
	/**
	 * Preprocess an expression tree.  We do a number of transformations
	 * here (including subqueries, IN lists, LIKE and BETWEEN) plus
	 * subquery flattening.
	 * NOTE: This is done before the outer ResultSetNode is preprocessed.
	 *
	 * @param	numTables			Number of tables in the DML Statement
	 * @param	outerFromList		FromList from outer query block
	 * @param	outerSubqueryList	SubqueryList from outer query block
	 * @param	outerPredicateList	PredicateList from outer query block
	 *
	 * @return		The modified expression
	 *
	 * @exception StandardException		Thrown on error
	 */
	public ValueNode preprocess(int numTables,
								FromList outerFromList,
								SubqueryList outerSubqueryList,
								PredicateList outerPredicateList) 
					throws StandardException
	{
		super.preprocess(numTables,
						 outerFromList, outerSubqueryList, 
						 outerPredicateList);

		/* If this is the first OR in the OR chain then we will
		 * consider converting it to an IN list and then performing
		 * whatever IN list conversions/optimizations are available.
		 * An OR can be converted to an IN list if all of the entries
		 * in the chain are of the form:
		 *		ColumnReference = x
		 *	or:
		 *		x = ColumnReference
		 * where all ColumnReferences are from the same table.
		 */
		if (firstOr)
		{
			boolean			convert = true;
// GemStone changes BEGIN
			final CompilerContext cc = getCompilerContext();
			boolean convertToMultiScan = cc
			    .orListOptimizationAllowed();
                        boolean queryHDFS = false;
                        if (lcc != null) {
                          queryHDFS = lcc.getQueryHDFS();
                        }
                        if (cc != null && cc.getHasQueryHDFS()) {
                          queryHDFS = cc.getQueryHDFS();
                        }
                        // See #49661
                        if (queryHDFS) {
                          convertToMultiScan = false;
                        }
			THashSet cols = null;
// Gemstone changes END
			ColumnReference	cr = null;
			int				columnNumber = -1;
			int				tableNumber = -1;

			for (ValueNode vn = this; vn instanceof OrNode; vn = ((OrNode) vn).getRightOperand())
			{
				OrNode on = (OrNode) vn;
				ValueNode left = on.getLeftOperand();

				// Is the operator an =
				if (!left.isRelationalOperator())
				{
					/* If the operator is an IN-list disguised as a relational
					 * operator then we can still convert it--we'll just
					 * combine the existing IN-list ("left") with the new IN-
					 * list values.  So check for that case now.
					 */ 

					if (SanityManager.DEBUG)
					{
						/* At the time of writing the only way a call to
						 * left.isRelationalOperator() would return false for
						 * a BinaryRelationalOperatorNode was if that node
						 * was for an IN-list probe predicate.  That's why we
						 * we can get by with the simple "instanceof" check
						 * below.  But if we're running in SANE mode, do a
						 * quick check to make sure that's still valid.
					 	 */
						BinaryRelationalOperatorNode bron = null;
						if (left instanceof BinaryRelationalOperatorNode)
						{
 							bron = (BinaryRelationalOperatorNode)left;
							if (!bron.isInListProbeNode())
							{
								SanityManager.THROWASSERT(
								"isRelationalOperator() unexpectedly returned "
								+ "false for a BinaryRelationalOperatorNode.");
							}
						}
					}

					convert = (left instanceof BinaryRelationalOperatorNode);
// GemStone changes BEGIN
					// check if the left is an AndNode having
					// relational operators as children
					if (!convert && convertToMultiScan) {
					  ValueNode valNode = left;
					  // TODO: PERF: currently only allowing for the same column
					  // in all the columns below an AND node or relational operator
					  // node; this is because need to add handling of additional
					  // qualifiers at the Index Scan Controllers layer, so once
					  // the optimization to remove IRTBRRS is done then remove
					  // this restriction
					  int colNumber = -1;
					  ArrayDeque<ValueNode> stack =
					    new ArrayDeque<ValueNode>();
					  for (;;) {
					    if (valNode instanceof AndNode) {
					      final AndNode an = (AndNode)valNode;
					      valNode = an.getLeftOperand();
					      if (valNode instanceof OrListNode) {
					        // convert back to OrNode
					        // TODO: PERF: convert to DNF and then handle
					        // as OrListNode at top-level
					        an.setLeftOperand(((OrListNode)valNode)
					            .getOrNode(0));
					        convertToMultiScan = false;
					        break;
					      }
					      stack.push(valNode);
					      final ValueNode rn = an.getRightOperand();
					      if (rn instanceof OrListNode) {
					        an.setRightOperand(((OrListNode)rn)
					            .getOrNode(0));
					        convertToMultiScan = false;
					        break;
					      }
					      stack.push(rn);
					      continue;
					    }
					    // TODO: PERF: currently only allowing relational ops with
					    // column refs; can change to also allow expressions which
					    // are ANDed with relational ops as additional qualifiers
					    // using ProjectRestrictNode kind of handling
					    else if (valNode instanceof BinaryRelationalOperatorNode) {
					      BinaryRelationalOperatorNode bron =
					        (BinaryRelationalOperatorNode)valNode;
					      ValueNode ln = bron.getLeftOperand();
					      if (ln instanceof ColumnReference) {
					        cr = (ColumnReference)ln;
					        if (tableNumber == -1) {
					          tableNumber = cr.getTableNumber();
					        }
					        else if (tableNumber != cr.getTableNumber()) {
					          convertToMultiScan = false;
					          break;
					        }
					        if (colNumber == -1) {
					          colNumber = cr.getColumnNumber();
					        }
					        else if (colNumber != cr.getColumnNumber()) {
					          convertToMultiScan = false;
					          break;
					        }
					      }
					      else if (!ln.isParameterNode() &&
					          !ln.isParameterizedConstantNode() &&
					          !ln.isConstantExpression()) {
					        convertToMultiScan = false;
					        break;
					      }
					      if (cr != null) {
					        if (cols == null) {
					          cols = new THashSet();
					        }
					        cols.add(cr);
					        cr = null;
					      }
					      ValueNode rn = bron.getRightOperand();
					      if (rn instanceof ColumnReference) {
					        cr = (ColumnReference)rn;
					        if (tableNumber == -1) {
					          tableNumber = cr.getTableNumber();
					        }
					        else if (tableNumber != cr.getTableNumber()) {
					          convertToMultiScan = false;
					          break;
					        }
					        if (colNumber == -1) {
					          colNumber = cr.getColumnNumber();
					        }
					        else if (colNumber != cr.getColumnNumber()) {
					          convertToMultiScan = false;
					          break;
					        }
					      }
					      else if (!rn.isParameterNode() &&
					          !rn.isParameterizedConstantNode() &&
					          !rn.isConstantExpression()) {
					        convertToMultiScan = false;
					        break;
					      }
					      if (cr != null) {
					        if (cols == null) {
					          cols = new THashSet();
					        }
					        cols.add(cr);
					      }
					    }
					    else if (valNode.isBooleanTrue()) {
					      // ignore the terminating boolean
					    }
					    else {
					      convertToMultiScan = false;
					      break;
					    }

					    if (stack.isEmpty()) {
					      break;
					    }
					    else {
					      valNode = stack.pop();
					    }
					  }
					}
					if (!convert) {
					  if (convertToMultiScan) {
					    continue;
					  }
					  else {
					    break;
					  }
					}
					/* (original code)
					if (!convert)
						break;
					*/
// GemStone changes END
				}

				if (!(((RelationalOperator)left).getOperator() == RelationalOperator.EQUALS_RELOP))
				{
					convert = false;
// GemStone changes BEGIN
					/* (original code)
					break;
					*/
// GemStone changes END
				}
// GemStone changes BEGIN
				if (!(left instanceof BinaryRelationalOperatorNode)) {
				  convertToMultiScan = false;
				  break;
				}
				ValueNode opNode;
// GemStone changes END

				BinaryRelationalOperatorNode bron = (BinaryRelationalOperatorNode)left;

				if (bron.getLeftOperand() instanceof ColumnReference)
				{
					cr = (ColumnReference) bron.getLeftOperand();
					if (tableNumber == -1)
					{
						tableNumber = cr.getTableNumber();
						columnNumber = cr.getColumnNumber();
					}
// GemStone changes BEGIN
					else if (tableNumber != cr.getTableNumber()) {
					  convert = false;
					  convertToMultiScan = false;
					  break;
					}
					else if (columnNumber != cr.getColumnNumber()) {
					  convert = false;
					}
					if (!(opNode = bron.getRightOperand()).isParameterNode() &&
					    !opNode.isParameterizedConstantNode() &&
					    !opNode.isConstantExpression()) {
					  convertToMultiScan = false;
					  if (!convert) {
					    break;
					  }
					}
					if (convertToMultiScan) {
					  if (cols == null) {
					    cols = new THashSet();
					  }
					  cols.add(cr);
					}
					/* (original code)
					else if (tableNumber != cr.getTableNumber() ||
							 columnNumber != cr.getColumnNumber())
					{
						convert = false;
						break;
					}
					*/
// GemStone changes END
				}
				else if (bron.getRightOperand() instanceof ColumnReference)
				{
					cr = (ColumnReference) bron.getRightOperand();
					if (tableNumber == -1)
					{
						tableNumber = cr.getTableNumber();
						columnNumber = cr.getColumnNumber();
					}
// GemStone changes BEGIN
					else if (tableNumber != cr.getTableNumber()) {
					  convert = false;
					  convertToMultiScan = false;
					  break;
					}
					else if (columnNumber != cr.getColumnNumber()) {
					  convert = false;
					}
					if (!(opNode = bron.getLeftOperand()).isParameterNode() &&
					    !opNode.isParameterizedConstantNode() &&
					    !opNode.isConstantExpression()) {
					  convertToMultiScan = false;
					  if (!convert) {
					    break;
					  }
					}
					if (convertToMultiScan) {
					  if (cols == null) {
					    cols = new THashSet();
					  }
					  cols.add(cr);
					}
					/* (original code)
					else if (tableNumber != cr.getTableNumber() ||
							 columnNumber != cr.getColumnNumber())
					{
						convert = false;
						break;
					}
					*/
// GemStone changes END
				}
				else
				{
					convert = false;
// GemStone changes BEGIN
					convertToMultiScan = false;
// GemStone changes END
					break;
				}
			}

			/* So, can we convert the OR chain? */
			if (convert)
			{
				ValueNodeList vnl = (ValueNodeList) getNodeFactory().getNode(
													C_NodeTypes.VALUE_NODE_LIST,
													getContextManager());
				// Build the IN list 
				for (ValueNode vn = this; vn instanceof OrNode; vn = ((OrNode) vn).getRightOperand())
				{
					OrNode on = (OrNode) vn;
					BinaryRelationalOperatorNode bron =
						(BinaryRelationalOperatorNode) on.getLeftOperand();
					if (bron.isInListProbeNode())
					{
						/* If we have an OR between multiple IN-lists on the same
						 * column then just combine them into a single IN-list.
						 * Ex.
						 *
						 *   select ... from T1 where i in (2, 3) or i in (7, 10)
						 *
						 * effectively becomes:
						 *
						 *   select ... from T1 where i in (2, 3, 7, 10).
						 */
						vnl.destructiveAppend(
							bron.getInListOp().getRightOperandList());
					}
					else if (bron.getLeftOperand() instanceof ColumnReference)
					{
						vnl.addValueNode(bron.getRightOperand());
					}
					else
					{
						vnl.addValueNode(bron.getLeftOperand());
					}
				}

				InListOperatorNode ilon =
							(InListOperatorNode) getNodeFactory().getNode(
											C_NodeTypes.IN_LIST_OPERATOR_NODE,
											cr,
											vnl,
											getContextManager());

				// Transfer the result type info to the IN list
				ilon.setType(getTypeServices());

				/* We return the result of preprocess() on the
				 * IN list so that any compilation time transformations
				 * will be done.
				 */
				return ilon.preprocess(numTables,
						 outerFromList, outerSubqueryList, 
						 outerPredicateList);
			}
// GemStone changes BEGIN
			// find the cases where OR can be converted to union
			// of table/index scans using MultiColumnTableScanRS;
			// currently we do this only when the query itself is
			// DNF like and not try to convert to DNF after/before
			// optimization
			else if (convertToMultiScan) {
			  ValueNodeList vnl = (ValueNodeList)getNodeFactory().getNode(
			      C_NodeTypes.VALUE_NODE_LIST,
			      getContextManager());
			  // Build the list to use for OrListNode
			  for (ValueNode vn = this; vn instanceof OrNode;
			      vn = ((OrNode)vn).getRightOperand()) {
			    vnl.addValueNode(vn);
			  }

			  OrListNode oln = (OrListNode)getNodeFactory().getNode(
			          C_NodeTypes.OR_LIST_NODE,
			          tableNumber,
			          cols,
			          vnl,
			          getContextManager());

			  // Transfer the result type info to the OR list
			  oln.setType(getTypeServices());
			  cc.setHasOrList(true);
			  return oln;
			}
// GemStone changes END
		}

		return this;
	}

	/**
	 * Eliminate NotNodes in the current query block.  We traverse the tree, 
	 * inverting ANDs and ORs and eliminating NOTs as we go.  We stop at 
	 * ComparisonOperators and boolean expressions.  We invert 
	 * ComparisonOperators and replace boolean expressions with 
	 * boolean expression = false.
	 * NOTE: Since we do not recurse under ComparisonOperators, there
	 * still could be NotNodes left in the tree.
	 *
	 * @param	underNotNode		Whether or not we are under a NotNode.
	 *							
	 *
	 * @return		The modified expression
	 *
	 * @exception StandardException		Thrown on error
	 */
// GemStone changes BEGIN
	public	ValueNode eliminateNots(boolean underNotNode) 
					throws StandardException
// GemStone changes END
	{
		leftOperand = leftOperand.eliminateNots(underNotNode);
		rightOperand = rightOperand.eliminateNots(underNotNode);
		if (! underNotNode)
		{
			return this;
		}

		/* Convert the OrNode to an AndNode */
		AndNode	andNode;

		andNode = (AndNode) getNodeFactory().getNode(
													C_NodeTypes.AND_NODE,
													leftOperand,
													rightOperand,
													getContextManager());
		andNode.setType(getTypeServices());
		return andNode;
	}

	/**
	 * Finish putting an expression into conjunctive normal
	 * form.  An expression tree in conjunctive normal form meets
	 * the following criteria:
	 *		o  If the expression tree is not null,
	 *		   the top level will be a chain of AndNodes terminating
	 *		   in a true BooleanConstantNode.
	 *		o  The left child of an AndNode will never be an AndNode.
	 *		o  Any right-linked chain that includes an AndNode will
	 *		   be entirely composed of AndNodes terminated by a true BooleanConstantNode.
	 *		o  The left child of an OrNode will never be an OrNode.
	 *		o  Any right-linked chain that includes an OrNode will
	 *		   be entirely composed of OrNodes terminated by a false BooleanConstantNode.
	 *		o  ValueNodes other than AndNodes and OrNodes are considered
	 *		   leaf nodes for purposes of expression normalization.
	 *		   In other words, we won't do any normalization under
	 *		   those nodes.
	 *
	 * In addition, we track whether or not we are under a top level AndNode.  
	 * SubqueryNodes need to know this for subquery flattening.
	 *
	 * @param	underTopAndNode		Whether or not we are under a top level AndNode.
	 *							
	 *
	 * @return		The modified expression
	 *
	 * @exception StandardException		Thrown on error
	 */
	public ValueNode changeToCNF(boolean underTopAndNode) 
					throws StandardException
	{
		OrNode curOr = this;

		/* If rightOperand is an AndNode, then we must generate an 
		 * OrNode above it.
		 */
		if (rightOperand instanceof AndNode)
		{
			BooleanConstantNode	falseNode;

			falseNode = (BooleanConstantNode) getNodeFactory().getNode(
											C_NodeTypes.BOOLEAN_CONSTANT_NODE,
											Boolean.FALSE,
											getContextManager());
			rightOperand = (ValueNode) getNodeFactory().getNode(
												C_NodeTypes.OR_NODE,
												rightOperand,
												falseNode,
												getContextManager());
			((OrNode) rightOperand).postBindFixup();
		}

		/* We need to ensure that the right chain is terminated by
		 * a false BooleanConstantNode.
		 */
		while (curOr.getRightOperand() instanceof OrNode)
		{
			curOr = (OrNode) curOr.getRightOperand();
		}

		/* Add the false BooleanConstantNode if not there yet */
		if (!(curOr.getRightOperand().isBooleanFalse()))
		{
			BooleanConstantNode	falseNode;

			falseNode = (BooleanConstantNode) getNodeFactory().getNode(
											C_NodeTypes.BOOLEAN_CONSTANT_NODE,
											Boolean.FALSE,
											getContextManager());
			curOr.setRightOperand(
					(ValueNode) getNodeFactory().getNode(
												C_NodeTypes.OR_NODE,
												curOr.getRightOperand(),
												falseNode,
												getContextManager()));
			((OrNode) curOr.getRightOperand()).postBindFixup();
		}

		/* If leftOperand is an OrNode, then we modify the tree from:
		 *
		 *				this
		 *			   /	\
		 *			Or2		Nodex
		 *		   /	\		...
		 *		left2	right2
		 *
		 *	to:
		 *
		 *						this
		 *					   /	\
		 *	left2.changeToCNF()		 Or2
		 *							/	\
		 *		right2.changeToCNF()	 Nodex.changeToCNF()
		 *
		 *	NOTE: We could easily switch places between left2.changeToCNF() and 
		 *  right2.changeToCNF().
		 */

		while (leftOperand instanceof OrNode)
		{
			ValueNode newLeft;
			OrNode	  oldLeft;
			OrNode	  newRight;
			ValueNode oldRight;

			/* For "clarity", we first get the new and old operands */
			newLeft = ((OrNode) leftOperand).getLeftOperand();
			oldLeft = (OrNode) leftOperand;
			newRight = (OrNode) leftOperand;
			oldRight = rightOperand;

			/* We then twiddle the tree to match the above diagram */
			leftOperand = newLeft;
			rightOperand = newRight;
			newRight.setLeftOperand(oldLeft.getRightOperand());
			newRight.setRightOperand(oldRight);
		}

		/* Finally, we continue to normalize the left and right subtrees. */
		leftOperand = leftOperand.changeToCNF(false);
		rightOperand = rightOperand.changeToCNF(false);

		return this;
	}

	/**
	 * Verify that changeToCNF() did its job correctly.  Verify that:
	 *		o  AndNode  - rightOperand is not instanceof OrNode
	 *				      leftOperand is not instanceof AndNode
	 *		o  OrNode	- rightOperand is not instanceof AndNode
	 *					  leftOperand is not instanceof OrNode
	 *
	 * @return		Boolean which reflects validity of the tree.
	 */
	public boolean verifyChangeToCNF()
	{
		boolean isValid = true;

		if (SanityManager.ASSERT)
		{
			isValid = ((rightOperand instanceof OrNode) ||
					   (rightOperand.isBooleanFalse()));
			if (rightOperand instanceof OrNode)
			{
				isValid = rightOperand.verifyChangeToCNF();
			}
			if (leftOperand instanceof OrNode)
			{
				isValid = false;
			}
			else
			{
				isValid = leftOperand.verifyChangeToCNF();
			}
		}

		return isValid;
	}

	/**
	 * Do bind() by hand for an AndNode that was generated after bind(),
	 * eg by putAndsOnTop(). (Set the data type and nullability info.)
	 *
	 * @exception StandardException		Thrown on error
	 */
	void postBindFixup()
					throws StandardException
	{
		setType(resolveLogicalBinaryOperator(
							leftOperand.getTypeServices(),
							rightOperand.getTypeServices()
											)
				);
	}
	// GemStone changes BEGIN
        @Override
        public int setColocatedWith(Optimizable src, Optimizable tgt, JBitSet srcPCols, JBitSet tgtPCols) throws StandardException {
          
          leftOperand.setColocatedWith(src, tgt, srcPCols, tgtPCols);
          rightOperand.setColocatedWith(src, tgt, srcPCols, tgtPCols);
          
          return UNKNOWN;
        }
        
        @Override
        public boolean categorize(JBitSet referencedTabs, boolean simplePredsOnly)
            throws StandardException {
          final CompilerContext cc = getCompilerContext();
          if (cc.isNCJoinOnRemote() && this.firstOr) {
            boolean isMultiTableCase = false;
            if (referencedTabs.isEmpty()) {
              JBitSet refTabMap = getTablesReferenced();
              if (!refTabMap.isEmpty()) {
                if (refTabMap.noOfBitsSet() > 1) {
                  isMultiTableCase = true;
                }
              }
            }
            else if (referencedTabs.noOfBitsSet() > 1) {
              isMultiTableCase = true;
            }
      
            if (isMultiTableCase) {
              return false;
            }
          }
      
          return super.categorize(referencedTabs, simplePredsOnly);
        }
        
        @Override
        public String ncjGenerateSql() {
          String rightSQL = rightOperand.ncjGenerateSql();
          String leftSQL = leftOperand.ncjGenerateSql();
          if (leftSQL == null || leftSQL.equalsIgnoreCase("false")
              || leftSQL.equalsIgnoreCase("true")) {
            if (rightSQL == null || rightSQL.equalsIgnoreCase("false")
                || rightSQL.equalsIgnoreCase("true")) {
              return null;
            }
            return rightSQL;
          }
          if (rightSQL == null || rightSQL.equalsIgnoreCase("false")
              || rightSQL.equalsIgnoreCase("true")) {
            if (leftSQL == null || leftSQL.equalsIgnoreCase("false")
                || leftSQL.equalsIgnoreCase("true")) {
              return null;
            }
            return leftSQL;
          }
      
          return "(" + leftSQL + " " + operator + " " + rightSQL + ")";
        }
	// GemStone changes END
}
