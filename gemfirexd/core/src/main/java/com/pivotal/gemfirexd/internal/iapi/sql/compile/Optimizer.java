/*

   Derby - Class com.pivotal.gemfirexd.internal.iapi.sql.compile.Optimizer

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

package com.pivotal.gemfirexd.internal.iapi.sql.compile;



import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.ConglomerateDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDictionary;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.TableDescriptor;

/**
 * Optimizer provides services for optimizing a query.
 * RESOLVE:
 *	o  Need to figure out what to do about subqueries, figuring out
 *	   their attachment points and how to communicate them back to the
 *	   caller.
 */

public interface Optimizer {
	/**
		Module name for the monitor's module locating system.
	 */
	String MODULE = "com.pivotal.gemfirexd.internal.iapi.sql.compile.Optimizer";

	/**
		Property name for controlling whether to do join order optimization.
	 */
	String JOIN_ORDER_OPTIMIZATION = "gemfirexd.optimizer.optimizeJoinOrder";

	/**
		Property name for controlling whether to do rule-based optimization,
		as opposed to cost-based optimization.
	 */
	String RULE_BASED_OPTIMIZATION =
						"gemfirexd.optimizer.ruleBasedOptimization";

	/**
		Property name for controlling whether the optimizer ever times out
		while optimizing a query and goes with the best plan so far.
	 */
	String NO_TIMEOUT = "gemfirexd.optimizer.noTimeout";

	/**
		Property name for controlling the maximum size of memory (in KB)
		the optimizer can use for each table.  If an access path takes
		memory larger than that size for a table, the access path is skipped.
		Default is 1024 (KB).
	 */
	String MAX_MEMORY_PER_TABLE = "gemfirexd.language.maxMemoryPerTable";

	/**
		Maximum size of dynamically created materialized rows. Caching large results
		use lot of memory and can cause stack overflow. See DERBY-634
	*/
	int MAX_DYNAMIC_MATERIALIZED_ROWS = 512;

	/**
	   Property name for disabling statistics use for all queries.
	*/
	String USE_STATISTICS = "gemfirexd.language.useStatistics";

	/** Indicates a "normal" plan that is not optimized to do sort avoidance */
	int NORMAL_PLAN = 1;

	/** Indicates a sort-avoidance plan */
	int SORT_AVOIDANCE_PLAN = 2;

	// optimizer trace
	public static final int STARTED = 1;
	public static final int TIME_EXCEEDED =2;
	public static final int NO_TABLES = 3;
	public static final int COMPLETE_JOIN_ORDER = 4;
	public static final int COST_OF_SORTING = 5;
	public static final int NO_BEST_PLAN = 6;
	public static final int MODIFYING_ACCESS_PATHS = 7;
	public static final int SHORT_CIRCUITING = 8;
	public static final int SKIPPING_JOIN_ORDER = 9;
	public static final int ILLEGAL_USER_JOIN_ORDER = 10;
	public static final int USER_JOIN_ORDER_OPTIMIZED = 11;
	public static final int CONSIDERING_JOIN_ORDER = 12;
	public static final int TOTAL_COST_NON_SA_PLAN = 13;
	public static final int TOTAL_COST_SA_PLAN = 14;
	public static final int TOTAL_COST_WITH_SORTING = 15;
	public static final int CURRENT_PLAN_IS_SA_PLAN = 16;
	public static final int CHEAPEST_PLAN_SO_FAR = 17;
	public static final int PLAN_TYPE = 18;
	public static final int COST_OF_CHEAPEST_PLAN_SO_FAR = 19;
	public static final int SORT_NEEDED_FOR_ORDERING = 20;
	public static final int REMEMBERING_BEST_JOIN_ORDER = 21;
	public static final int SKIPPING_DUE_TO_EXCESS_MEMORY = 22;
	public static final int COST_OF_N_SCANS = 23;
	public static final int HJ_SKIP_NOT_MATERIALIZABLE = 24;
	public static final int HJ_SKIP_NO_JOIN_COLUMNS = 25;
	public static final int HJ_HASH_KEY_COLUMNS = 26;
	public static final int CALLING_ON_JOIN_NODE = 27;
	public static final int CONSIDERING_JOIN_STRATEGY = 28;
	public static final int REMEMBERING_BEST_ACCESS_PATH = 29;
	public static final int NO_MORE_CONGLOMERATES = 30;
	public static final int CONSIDERING_CONGLOMERATE = 31;
	public static final int SCANNING_HEAP_FULL_MATCH_ON_UNIQUE_KEY = 32;
	public static final int ADDING_UNORDERED_OPTIMIZABLE = 33;
	public static final int CHANGING_ACCESS_PATH_FOR_TABLE = 34;
	public static final int TABLE_LOCK_NO_START_STOP = 35;
	public static final int NON_COVERING_INDEX_COST = 36;
	public static final int ROW_LOCK_ALL_CONSTANT_START_STOP = 37;
	public static final int ESTIMATING_COST_OF_CONGLOMERATE = 38;
	public static final int LOOKING_FOR_SPECIFIED_INDEX = 39;
	public static final int MATCH_SINGLE_ROW_COST = 40;
	public static final int COST_INCLUDING_EXTRA_1ST_COL_SELECTIVITY = 41;
	public static final int CALLING_NEXT_ACCESS_PATH = 42;
	public static final int TABLE_LOCK_OVER_THRESHOLD = 43;
	public static final int ROW_LOCK_UNDER_THRESHOLD = 44;
	public static final int COST_INCLUDING_EXTRA_START_STOP = 45;
	public static final int COST_INCLUDING_EXTRA_QUALIFIER_SELECTIVITY = 46;
	public static final int COST_INCLUDING_EXTRA_NONQUALIFIER_SELECTIVITY = 47;
	public static final int COST_OF_NONCOVERING_INDEX = 48;
	public static final int REMEMBERING_JOIN_STRATEGY = 49;
	public static final int REMEMBERING_BEST_ACCESS_PATH_SUBSTRING = 50;
	public static final int REMEMBERING_BEST_SORT_AVOIDANCE_ACCESS_PATH_SUBSTRING = 51;
	public static final int REMEMBERING_BEST_UNKNOWN_ACCESS_PATH_SUBSTRING = 52;
	public static final int COST_OF_CONGLOMERATE_SCAN1 = 53;
	public static final int COST_OF_CONGLOMERATE_SCAN2 = 54;
	public static final int COST_OF_CONGLOMERATE_SCAN3 = 55;
	public static final int COST_OF_CONGLOMERATE_SCAN4 = 56;
	public static final int COST_OF_CONGLOMERATE_SCAN5 = 57;
	public static final int COST_OF_CONGLOMERATE_SCAN6 = 58;
	public static final int COST_OF_CONGLOMERATE_SCAN7 = 59;
	public static final int COST_INCLUDING_COMPOSITE_SEL_FROM_STATS= 60;
	public static final int COMPOSITE_SEL_FROM_STATS = 61;
	public static final int COST_INCLUDING_STATS_FOR_INDEX = 62;
// GemStone changes BEGIN
	/**
	 * Indicates that hash index is being considered for sort order
	 * due to unique single row match.
	 */
	public static final int HASH_INDEX_FULL_MATCH_ON_UNIQUE_KEY = 63;
	/** indicates hash join being skipped on an index */
	public static final int HJ_SKIP_INDEX = 64;
	
	/**Replicated tables can be colocated with Any driver table*/
	public static final int SETTING_REPLICATED_TABLE_GROUP = 65;
	
	/** logs when colocated table groups are reset when driver table changes. */
	public static final int RESETTING_DRIVER_TABLE_INDEX = 66;
	
// GemStone changes END
	/**
	 * Iterate through the permutations, returning false when the permutations
	 * are exhausted.
	 * NOTE - Implementers are responsible for hiding tree pruning of permutations
	 * behind this method call.
	 *
	 * @return boolean	True - An optimizable permutation remains.
	 *					False - Permutations are exhausted.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public boolean getNextPermutation() throws StandardException;

	/**
	 * Iterate through the "decorated permutations", returning false when they
	 * are exhausted.
	 * NOTE - Implementers are responsible for hiding tree pruning of access
	 * methods behind this method call.
	 *
	 * @return boolean	True - An optimizable decorated permutation remains.
	 *					False - Decorated permutations are exhausted.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public boolean getNextDecoratedPermutation() throws StandardException;

	/**
	 * Cost the current permutation.
	 * Caller is responsible for pushing all predicates which can be evaluated 
	 * prior to costing.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void costPermutation() throws StandardException;

	/**
	 * Cost the current Optimizable with the specified OPL.
	 * Caller is responsible for pushing all predicates which can be evaluated 
	 * prior to costing.
	 *
	 * @param optimizable	The Optimizable
	 * @param td			TableDescriptor of the Optimizable
	 * @param cd			The ConglomerateDescriptor for the conglom to cost
	 *						(This should change to an object to represent
	 *						access paths, but for now this is OK).
	 * @param predList		The OptimizablePredicateList to apply
	 * @param outerCost		The cost of the tables outer to the one being
	 *						optimizer - tells how many outer rows there are.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void	costOptimizable(Optimizable optimizable,
								TableDescriptor td, 
								ConglomerateDescriptor cd,
								OptimizablePredicateList predList,
								CostEstimate outerCost)
			throws StandardException;

	/**
	 * Consider the cost of the given optimizable.  This method is like
	 * costOptimizable, above, but it is used when the Optimizable does
	 * not need help from the optimizer in costing the Optimizable (in practice,
	 * all Optimizables except FromBaseTable use this method.
	 *
	 * Caller is responsible for pushing all predicates which can be evaluated 
	 * prior to costing.
	 *
	 * @param optimizable	The Optimizable
	 * @param predList		The OptimizablePredicateList to apply
	 * @param estimatedCost	The estimated cost of the given optimizable
	 * @param outerCost		The cost of the tables outer to the one being
	 *						optimizer - tells how many outer rows there are.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void	considerCost(Optimizable optimizable,
							OptimizablePredicateList predList,
							CostEstimate estimatedCost,
							CostEstimate outerCost)
			throws StandardException;

	/**
	 * Return the DataDictionary that the Optimizer is using.
	 * This is useful when an Optimizable needs to call optimize() on
	 * a child ResultSetNode.
	 * 
	 * @return DataDictionary	DataDictionary that the Optimizer is using.
	 */
	public DataDictionary getDataDictionary();

	/**
	 * Modify the access path for each Optimizable, as necessary.  This includes
	 * things like adding result sets to translate from index rows to base rows.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void modifyAccessPaths() throws StandardException;

	/** Get a new CostEstimate object */
	public CostEstimate newCostEstimate();

	/** Get the estimated cost of the optimized query */
	public CostEstimate getOptimizedCost();

	/**
	 * Get the final estimated cost of the optimized query.  This
	 * should be the cost that corresponds to the best overall join
	 * order chosen by the optimizer, and thus this method should
	 * only be called after optimization is complete (i.e. when
	 * modifying access paths).
	 */
	public CostEstimate getFinalCost();

	/**
	 * Prepare for another round of optimization.
	 *
	 * This method is called before every "round" of optimization, where
	 * we define a "round" to be the period between the last time a call to
	 * getOptimizer() (on either a ResultSetNode or an OptimizerFactory)
	 * returned _this_ Optimizer and the time a call to this Optimizer's
	 * getNextPermutation() method returns FALSE.  Any re-initialization
	 * of state that is required before each round should be done in this
	 * method.
	 */
	public void prepForNextRound();

	/**
	 * Set the estimated number of outer rows - good for optimizing nested
	 * optimizables like subqueries and join nodes.
	 */
	public void setOuterRows(double outerRowCount);

	/**
	 * Get the number of join strategies supported by this optimizer.
	 */
	public int getNumberOfJoinStrategies();

	/**
	 * Get the maximum number of estimated rows touched in a table before
	 * we decide to open the table with table locking (as opposed to row
	 * locking.
	 */
	public int tableLockThreshold();

	/**
	 * Gets a join strategy by number (zero-based).
	 */
	JoinStrategy getJoinStrategy(int whichStrategy);

	/**
	 * Gets a join strategy by name.  Returns null if not found.
	 * The look-up is case-insensitive.
	 */
	JoinStrategy getJoinStrategy(String whichStrategy);

	/**
	 * Optimizer trace.
	 */
	public void trace(int traceFlag, int intParam1, int intParam2,
					  double doubleParam, Object objectParam1);

	/**
	 * Get the level of this optimizer.
	 *
	 * @return The level of this optimizer.
	 */
	public int getLevel();

	/**
	 * Tells whether any of the tables outer to the current one
	 * has a uniqueness condition on the given predicate list,
	 * and if so, how many times each unique key can be seen by
	 * the current table.
	 *
	 * @param predList		The predicate list to check
	 *
	 * @return	<= 0 means there is no uniqueness condition
	 *			> 0 means there is a uniqueness condition on an
	 *			outer table, and the return value is the reciprocal of
	 *			the maximum number of times the optimizer estimates that each
	 *			unique key will be returned. For example, 0.5 means the
	 *			optimizer thinks each distinct join key will be returned
	 *			at most twice.
	 *
	 * @exception StandardException		Thrown on error
	 */
	double uniqueJoinWithOuterTable(OptimizablePredicateList predList)
			throws StandardException;
	
	/** 
	 * If statistics should be considered by the optimizer while optimizing 
	 * a query. The user may disable the use of statistics by setting the
	 * property gemfirexd.optimizer.useStatistics or by using the property
	 * useStatistics in a query.
	 *
	 * @see #USE_STATISTICS
	 */
	public boolean useStatistics();

    /**
     * @return the maximum number of bytes to be used per table.
     */
    public int getMaxMemoryPerTable();

    // GemStone changes BEGIN
    /**
     * Determines whether user defined join predicate(s) is complete w.r.t
     * colocation of current optimizable with the driver table group. <br> <br>
     * 
     * Two table are eligible for local-only execution iff, all partitioning
     * columns of both the tables have join relation defined with AND expression. <br> <br>
     * 
     * e.g. if 2 tables are partitioned as follows <br> <br>
     *  A(pc1), A(pc2) B(pc3), B(pc4) <br> <br>
     *  
     * then a query having below join condition is only eligible for colocated-join
     * i.e. each node can evaluate the join relation without any data fetch from other
     * nodes. <br> <br>
     * 
     *  <code> select * from A, B where A.pc1 = B.pc3 AND A.pc2 = B.pc4 </code>
     *   <br> <br>
     * <b>Note:</b> Join condition with OR clause in above query is again a distributed case.
     * 
     * @throws StandardException
     */
      public void determineColocation()
          throws StandardException;
      
      /**
       * Checks whether atleast one Non-colocated table(s) a.k.a remote table(s) is
       * present in the query.
       * 
       * @return true if distributed join involved else false
       */
      public boolean hasNonColocatedTables();
      
      /**
       * Save the PrIDs colocation groups as determined
       * by bestJoinOrder.
       * 
       * This method is required as Optimizable in the optimizableList
       * gets in-place modified during {@link #modifyAccessPaths()}. 
       * Therefore, this method should be invoked before modifying the
       * tree according to bestJoinOrder.
       * 
       * @throws StandardException 
       */
      public void saveBestJoinOrderPrIDs() throws StandardException;
    // GemStone changes END


}
