package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.PredicateOperator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.query.expr.Expression;
import edu.berkeley.cs186.database.query.join.BNLJOperator;
import edu.berkeley.cs186.database.query.join.SNLJOperator;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

import java.util.*;

/**
 * 查询计划是查询运算符的组合，它提供了查询的执行方式。
 * 如果嵌套select，如何执行呢？？
 * 默认使用别名即可。你一旦起了别名就只能用这个别名！不可以同时有多个table_name,在MySQL中也是这样的。
 * QueryPlan provides a set of functions to generate simple queries. Calling the
 * methods corresponding to SQL syntax stores the information in the QueryPlan,
 * and calling execute generates and executes a QueryPlan DAG.
 */
public class QueryPlan {
    // The transaction this query will be executed within
    private TransactionContext transaction;
    // A query operator representing the final query plan
    private QueryOperator finalOperator;
    // A list of columns to output (SELECT clause)
    private List<String> projectColumns;
    // Used by command line version to pass expressions to evaluate
    private List<Expression> projectFunctions;
    // A list of aliased table names involved in this query (FROM clause)
    private List<String> tableNames;
    // A list of objects representing joins (INNER JOIN clauses)
    private List<JoinPredicate> joinPredicates;
    // A map from aliases to tableNames (tableName AS alias)
    // 此map包含了所有表的别名，没有别名的话默认的别名就是原始表名
    private Map<String, String> aliases;
    // Aliases for temporary tables from WITH clause
    // With子句中的临时表表名
    private Map<String, String> cteAliases;
    // A list of objects representing selection predicates (WHERE clause)
    private List<SelectPredicate> selectPredicates;
    // A list of columns to group by (GROUP BY clause)
    private List<String> groupByColumns;
    // Column to sort on
    private String sortColumn;
    // A limit to the number of records yielded (LIMIT clause)
    private int limit;
    // An offset to the records yielded (OFFSET clause)
    private int offset;

    /**
     * Creates a new QueryPlan within `transaction` with base table
     * `baseTableName`
     *
     * @param transaction the transaction containing this query
     * @param baseTableName the source table for this query
     */
    public QueryPlan(TransactionContext transaction, String baseTableName) {
        this(transaction, baseTableName, baseTableName);
    }

    /**
     * Creates a new QueryPlan within transaction and base table startTableName
     * aliased as aliasTableName.
     *
     * @param transaction the transaction containing this query
     * @param baseTableName the source table for this query
     * @param aliasTableName the alias for the source table
     */
    public QueryPlan(TransactionContext transaction, String baseTableName,
                     String aliasTableName) {
        this.transaction = transaction;

        // Our tables so far just consist of the base table
        this.tableNames = new ArrayList<>();
        this.tableNames.add(aliasTableName);

        // Handle aliasing
        this.aliases = new HashMap<>();
        this.cteAliases = new HashMap<>();
        this.aliases.put(aliasTableName, baseTableName);
        this.transaction.setAliasMap(this.aliases);

        // These will be populated as the user adds projects, selects, etc...
        this.projectColumns = new ArrayList<>();
        this.projectFunctions = null;
        this.joinPredicates = new ArrayList<>();
        this.selectPredicates = new ArrayList<>();
        this.groupByColumns = new ArrayList<>();
        this.limit = -1;
        this.offset = 0;

        // This will be set after calling execute()
        this.finalOperator = null;
    }

    public QueryOperator getFinalOperator() {
        return this.finalOperator;
    }

    /**
     * @param column the name of an ambiguous column that we want to determine
     *               the table of.
     * @return the table the column belongs to
     * @throws IllegalArgumentException if the column name is ambiguous (it belongs
     * to two or more tables in this.tableNames) or if its completely unknown
     * (it didn't belong to any of the tables in this.tableNames)
     */
    private String resolveColumn(String column) {
        String result = null;
        for (String tableName : this.tableNames) {
            Schema s = transaction.getSchema(tableName);
            for (String fieldName : s.getFieldNames()) {
                if (fieldName.equals(column)) {
                    if (result != null) throw new RuntimeException(
                            "Ambiguous column name `" + column + " found in both `" +
                                    result + "` and `" + tableName + "`.");
                    result = tableName;
                }
            }
        }
        if (result == null)
            throw new IllegalArgumentException("Unknown column `" + column + "`");
        return result;
    }

    @Override
    public String toString() {
        // Comically large toString() function. Formats the QueryPlan attributes
        // into SQL query format.
        StringBuilder result = new StringBuilder();
        // SELECT clause
        if (this.projectColumns.size() == 0) result.append("SELECT *");
        else {
            result.append("SELECT ");
            result.append(String.join(", ", projectColumns));
        }
        // FROM clause
        String baseTable = this.tableNames.get(0);
        String alias = aliases.get(baseTable);
        if (baseTable.equals(aliases.get(baseTable)))
            result.append(String.format("\nFROM %s\n", baseTable));
        else result.append(String.format("\nFROM %s AS %s\n", baseTable, alias));
        // INNER JOIN clauses
        for (JoinPredicate predicate : this.joinPredicates)
            result.append(String.format("    %s\n", predicate));
        // WHERE clause
        if (selectPredicates.size() > 0) {
            result.append("WHERE\n");
            List<String> predicates = new ArrayList<>();
            for (SelectPredicate predicate : this.selectPredicates) {
                predicates.add(predicate.toString());
            }
            result.append("   ").append(String.join(" AND\n   ", predicates));
            result.append("\n");
        }
        // GROUP BY clause
        if (this.groupByColumns.size() > 0) {
            result.append("GROUP BY ");
            result.append(String.join(", ", groupByColumns));
            result.append("\n");
        }
        result.append(";");
        return result.toString();
    }

    // Helper Classes //////////////////////////////////////////////////////////

    /**
     * Represents a single selection predicate. Some examples:
     *   table1.col = 186
     *   table2.col <= 123
     *   table3.col > 6
     */
    private class SelectPredicate {
        //指定需要断言的表和字段
        String tableName;
        String column;
        //断言
        PredicateOperator operator;
        //进行比较的值
        DataBox value;

        /**
         *
         * @param column-具有两种形式，1）tableName.columnName，即包含了表名; 2) 不包含表名，需要根据列名寻找包含此列的表，不是很安全。
         * @param operator
         * @param value
         */
        SelectPredicate(String column, PredicateOperator operator, DataBox value) {
            if (column.contains(".")) {
                this.tableName = column.split("\\.")[0];
                column = column.split("\\.")[1];
            } else this.tableName = resolveColumn(column);
            this.column = column;
            this.operator = operator;
            this.value = value;
        }

        @Override
        public String toString() {
            return String.format("%s.%s %s %s", tableName, column, operator.toSymbol(), value);
        }
    }

    /**
     * Represents an equijoin in the query plan. Some examples:
     *   INNER JOIN rightTable ON leftTable.leftColumn = rightTable.rightColumn
     *   INNER JOIN table2 ON table2.some_id = table1.some_id
     */
    private class JoinPredicate {
        String leftTable;
        /** 左表的全限定连接字段名，即包含了表名*/
        String leftColumn;
        String rightTable;
        String rightColumn;
        private String joinTable; // Just for formatting purposes

        JoinPredicate(String tableName, String leftColumn, String rightColumn) {
            if (!leftColumn.contains(".") || !rightColumn.contains(".")) {
                throw new IllegalArgumentException("Join columns must be fully qualified");
            }

            // The splitting logic below just separates the column name from the
            // table name.
            this.joinTable = tableName;
            this.leftTable = leftColumn.split("\\.")[0];
            this.leftColumn = leftColumn;
            this.rightTable = rightColumn.split("\\.")[0];
            this.rightColumn = rightColumn;
            if (!tableName.equals(rightTable) && !tableName.equals(leftTable)) {
                throw new IllegalArgumentException(String.format(
                        "`%s` is invalid. ON clause of INNER JOIN must contain the " +
                                "new table being joined.",
                        this.toString()
                ));
            }
        }

        @Override
        public String toString() {
            String unAliased = aliases.get(joinTable);
            if (unAliased.equals(joinTable)) {
                return String.format("INNER JOIN %s ON %s = %s",
                        this.joinTable, this.leftColumn, this.rightColumn);
            }
            return String.format("INNER JOIN %s AS %s ON %s = %s",
                    unAliased, this.joinTable, this.leftColumn, this.rightColumn);
        }
    }

    // Project /////////////////////////////////////////////////////////////////

    /**
     * Add a project operator to the QueryPlan with the given column names.
     *
     * @param columnNames the columns to project
     * @throws RuntimeException a set of projections have already been
     * specified.
     */
    public void project(String... columnNames) {
        project(Arrays.asList(columnNames));
    }

    /**
     * 添加一个投影操作符
     * <br>
     * Add a project operator to the QueryPlan with a list of column names. Can
     * only specify one set of projections.
     *
     * @param columnNames the columns to project
     * @throws RuntimeException a set of projections have already been
     * specified.
     */
    public void project(List<String> columnNames) {
        if (!this.projectColumns.isEmpty()) {
            throw new RuntimeException(
                    "Cannot add more than one project operator to this query."
            );
        }
        if (columnNames.isEmpty()) {
            throw new RuntimeException("Cannot project no columns.");
        }
        this.projectColumns = new ArrayList<>(columnNames);
    }

    public void project(List<String> names, List<Expression> functions) {
        this.projectColumns = names;
        this.projectFunctions = functions;
    }

    /**
     * Sets the final operator to a project operator with the original final
     * operator as its source. Does nothing if there are no project columns.
     */
    private void addProject() {
        if (!this.projectColumns.isEmpty()) {
            if (this.finalOperator == null) throw new RuntimeException(
                    "Can't add Project onto null finalOperator."
            );
            if (this.projectFunctions == null) {
                this.finalOperator = new ProjectOperator(
                        this.finalOperator,
                        this.projectColumns,
                        this.groupByColumns
                );
            } else {
                this.finalOperator = new ProjectOperator(
                        this.finalOperator,
                        this.projectColumns,
                        this.projectFunctions,
                        this.groupByColumns
                );
            }
        }
    }

    // Sort ////////////////////////////////////////////////////////////////////

    /**
     * Add a sort operator to the query plan on the given column.
     */
    public void sort(String sortColumn) {
        if (sortColumn == null) throw new UnsupportedOperationException("Only one sort column supported");
        this.sortColumn = sortColumn;
    }

    /**
     * Sets the final operator to a sort operator if a sort was specified and
     * the final operator isn't already sorted.
     */
    private void addSort() {
        if (this.sortColumn == null) return;
        if (this.finalOperator.sortedBy().contains(sortColumn.toLowerCase())) {
            return; // already sorted
        }
        this.finalOperator = new SortOperator(
                this.transaction,
                this.finalOperator,
                this.sortColumn
        );
    }

    // Limit ///////////////////////////////////////////////////////////////////

    /**
     * Add a limit with no offset
     * @param limit an upper bound on the number of records to be yielded
     */
    public void limit(int limit) {
        this.limit(limit, 0);
    }

    /**
     * Add a limit with an offset
     * @param limit an upper bound on the number of records to be yielded
     * @param offset discards this many records before yielding the first one
     */
    public void limit(int limit, int offset) {
        this.limit = limit;
        this.offset = offset;
    }

    /**
     * Sets the final operator to a limit operator with the original final
     * operator as its source. Does nothing if limit is negative.
     */
    private void addLimit() {
        if (this.limit >= 0) {
            this.finalOperator = new LimitOperator(
                    this.finalOperator,
                    this.limit, this.offset
            );
        }
    }

    // Select //////////////////////////////////////////////////////////////////

    /**
     * 添加一个选择操作符，对应的列要满足谓词operator-value
     * <br>
     * Add a select operator. Only returns columns in which the column fulfills
     * the predicate relative to value.
     *
     * @param column the column to specify the predicate on
     * @param operator the operator of the predicate (=, <, <=, >, >=, !=)
     * @param value the value to compare against
     */
    public void select(String column, PredicateOperator operator,
                       Object value) {
        DataBox d = DataBox.fromObject(value);
        this.selectPredicates.add(new SelectPredicate(column, operator, d));
    }

    /**
     * For each selection predicate:
     * - creates a project operator with the final operator as its source
     * - sets the current final operator to the new project operator
     */
    private void addSelectsNaive() {
        for (int i = 0; i < selectPredicates.size(); i++) {
            SelectPredicate predicate = selectPredicates.get(i);
            this.finalOperator = new SelectOperator(
                    this.finalOperator,
                    predicate.tableName + "." + predicate.column,
                    predicate.operator,
                    predicate.value
            );
        }
    }

    // Group By ////////////////////////////////////////////////////////////////

    /**
     * Set the group by columns for this query.
     *
     * @param columns the columns to group by
     */
    public void groupBy(String... columns) {
        this.groupByColumns = Arrays.asList(columns);
    }

    /**
     * Set the group by columns for this query.
     *
     * @param columns the columns to group by
     */
    public void groupBy(List<String> columns) {
        this.groupByColumns = columns;
    }

    /**
     * Sets the final operator to a GroupByOperator with the original final
     * operator as its source. Does nothing there are no group by columns.
     */
    private void addGroupBy() {
        if (this.groupByColumns.size() > 0) {
            if (this.finalOperator == null) throw new RuntimeException(
                    "Can't add GroupBy onto null finalOperator."
            );
            this.finalOperator = new GroupByOperator(
                    this.finalOperator,
                    this.transaction,
                    this.groupByColumns
            );
        }
    }

    // Join ////////////////////////////////////////////////////////////////////

    /**
     * Join操作会引入另一张table，用tableName表示新加入的表
     * <br>
     * Join the leftColumnName column of the existing query plan against the
     * rightColumnName column of tableName.
     *
     * @param tableName the table to join against
     * @param leftColumnName the join column in the existing QueryPlan - 当前QueryPlan中的连接列
     * @param rightColumnName the join column in tableName - tableName中的连接列
     */
    public void join(String tableName, String leftColumnName, String rightColumnName) {
        join(tableName, tableName, leftColumnName, rightColumnName);
    }

    /**
     * Join the leftColumnName column of the existing queryplan against the
     * rightColumnName column of tableName, aliased as aliasTableName.
     *
     * @param tableName the table to join against
     * @param aliasTableName alias of table to join against
     * @param leftColumnName the join column in the existing QueryPlan,include tableName and columnName
     * @param rightColumnName the join column in tableName
     */
    public void join(String tableName, String aliasTableName, String leftColumnName,
                     String rightColumnName) {
        if (this.aliases.containsKey(aliasTableName)) {
            throw new RuntimeException("table/alias " + aliasTableName + " already in use");
        }
        if (cteAliases.containsKey(tableName)) {
            tableName = cteAliases.get(tableName);
        }
        this.aliases.put(aliasTableName, tableName);
        this.joinPredicates.add(new JoinPredicate(
                aliasTableName,
                leftColumnName,
                rightColumnName
        ));
        this.tableNames.add(aliasTableName);
        this.transaction.setAliasMap(this.aliases);
    }

    /**
     * For each table in this.joinTableNames
     * - creates a sequential scan operator over the table
     * - joins the current final operator with the sequential scan
     * - sets the final operator to the join
     */
    private void addJoinsNaive() {
        int pos = 1;
        for (JoinPredicate predicate : joinPredicates) {
            this.finalOperator = new SNLJOperator(
                    finalOperator,
                    new SequentialScanOperator(
                            this.transaction,
                            tableNames.get(pos)
                    ),
                    predicate.leftColumn,
                    predicate.rightColumn,
                    this.transaction
            );
            pos++;
        }
    }

    public void addTempTableAlias(String tableName, String alias) {
        if (cteAliases.containsKey(alias)) {
            throw new UnsupportedOperationException("Duplicate alias " + alias);
        }
        cteAliases.put(alias, tableName);
        for (String k : aliases.keySet()) {
            if (aliases.get(k).toLowerCase().equals(alias.toLowerCase())) {
                aliases.put(k, tableName);
            }
        }
        this.transaction.setAliasMap(this.aliases);
    }

    // Task 5: Single Table Access Selection ///////////////////////////////////

    /**
     * 获取与此表相关的，且谓词运算符可用于索引扫描的所有选择谓词。
     * <br>
     * Gets all select predicates for which there exists an index on the column
     * referenced in that predicate for the given table and where the predicate
     * operator can be used in an index scan.
     *
     * @return a list of indices of eligible selection predicates in
     * this.selectPredicates
     */
    private List<Integer> getEligibleIndexColumns(String table) {
        List<Integer> result = new ArrayList<>();
        for (int i = 0; i < this.selectPredicates.size(); i++) {
            SelectPredicate p = this.selectPredicates.get(i);
            // ignore if the selection predicate is for a different table
            if (!p.tableName.equals(table)) continue;
            boolean indexExists = this.transaction.indexExists(table, p.column);
            boolean canScan = p.operator != PredicateOperator.NOT_EQUALS;
            if (indexExists && canScan) result.add(i);
        }
        return result;
    }

    /**
     * 将所有符合条件的选择谓词下放到source,但除去索引处的谓词，因为这个谓词已经应用到索引扫描了，没必要再次应用。
     * 这并不难理解，IndexOperator也是一个迭代器，但他显然并不是全局扫描，他的特点就是索引，所以肯定是涉及到索引处的选择才会使用IndexOperator，
     * 如果不涉及索引，那根本不需要使用它。所以要传递给IndexOperator一个SelectPredicate供它查找索引以及迭代数据，换句话说传递过去这个谓词已经应用
     * 到数据上了。所以这里在下放select时，对于索引操作符就可以舍去他对应的那个select谓词，这就是except参数的作用。
     * <br>
     * Applies all eligible select predicates to a given source, except for the
     * predicate at index except. The purpose of except is because there might
     * be one select predicate that was already used for an index scan, so
     * there's no point applying it again. A select predicate is represented as
     * an element in this.selectPredicates. `except` corresponds to the index
     * of the predicate in that list.
     *
     * @param source a source operator to apply the selections to
     * @param except the index of a selection to skip. You can use the value -1
     *               if you don't want to skip anything.
     * @return a new query operator after select predicates have been applied
     */
    private QueryOperator addEligibleSelections(QueryOperator source, int except) {
        for (int i = 0; i < this.selectPredicates.size(); i++) {
            if (i == except) continue;
            SelectPredicate curr = this.selectPredicates.get(i);
            try {
                String colName = source.getSchema().matchFieldName(curr.tableName + "." + curr.column);
                source = new SelectOperator(
                        source, colName, curr.operator, curr.value
                );
            } catch (RuntimeException err) {
                /* do nothing */
            }
        }
        return source;
    }

    /**
     * Finds the lowest cost QueryOperator that accesses the given table. First
     * determine the cost of a sequential scan for the given table. Then for
     * every index that can be used on that table, determine the cost of an
     * index scan. Keep track of the minimum cost operation and push down
     * eligible select predicates.
     *
     * If an index scan was chosen, exclude the redundant select predicate when
     * pushing down selects. This method will be called during the first pass of
     * the search algorithm to determine the most efficient way to access each
     * table.
     *
     * @return a QueryOperator that has the lowest cost of scanning the given
     * table which is either a SequentialScanOperator or an IndexScanOperator
     * nested within any possible pushed down select operators. Ties for the
     * minimum cost operator can be broken arbitrarily.
     */
    public QueryOperator minCostSingleAccess(String table) {
//        table = aliases.get(table);
        QueryOperator minOp = new SequentialScanOperator(this.transaction, table);
        // TODO(proj3_part2): implement
        int minCost = minOp.estimateIOCost();
        int except = -1;
        for (int i : getEligibleIndexColumns(table)) {
            SelectPredicate selectPredicate = selectPredicates.get(i);
            QueryOperator tmpOp = new IndexScanOperator(this.transaction, table, selectPredicate.column,
                    selectPredicate.operator, selectPredicate.value);
            int tmpCost = tmpOp.estimateIOCost();
            if (tmpCost < minCost) {
                minCost = tmpCost;
                minOp = tmpOp;
                except = i;
            }
        }
        minOp = addEligibleSelections(minOp, except);
        return minOp;
    }

    // Task 6: Join Selection //////////////////////////////////////////////////

    /**
     * 给定两个QueryOperator，找出最优的连接方式，这里只考虑了SNLJ和BNLJ。
     * <br>
     * Given a join predicate between left and right operators, finds the lowest
     * cost join operator out of join types in JoinOperator.JoinType. By default
     * only considers SNLJ and BNLJ to prevent dependencies on GHJ, Sort and SMJ.
     *
     * Reminder: Your implementation does not need to consider cartesian products
     * and does not need to keep track of interesting orders.
     *
     * @return lowest cost join QueryOperator between the input operators
     */
    private QueryOperator minCostJoinType(QueryOperator leftOp,
                                          QueryOperator rightOp,
                                          String leftColumn,
                                          String rightColumn) {
        QueryOperator bestOperator = null;
        int minimumCost = Integer.MAX_VALUE;
        List<QueryOperator> allJoins = new ArrayList<>();
        allJoins.add(new SNLJOperator(leftOp, rightOp, leftColumn, rightColumn, this.transaction));
        allJoins.add(new BNLJOperator(leftOp, rightOp, leftColumn, rightColumn, this.transaction));
        for (QueryOperator join : allJoins) {
            int joinCost = join.estimateIOCost();
            if (joinCost < minimumCost) {
                bestOperator = join;
                minimumCost = joinCost;
            }
        }
        return bestOperator;
    }

    /**
     * 这里之所以没有枚举所有的table，而是枚举join predicate，原因在于在课程中说明了，我们的优化计划中不包含叉积，
     * 而不是join predicate中的任意两表之间连接，那就是叉积，因为没有这俩表的连接条件。例如 A join B join C，
     * (A,B),(B,C)之间均有连接条件，但对于AC是没有的，要考虑所有结果，即叉积。
     * <br>
     * Iterate through all table sets in the previous pass of the search. For
     * each table set, check each join predicate to see if there is a valid join
     * with a new table. If so, find the minimum cost join. Return a map from
     * each set of table names being joined to its lowest cost join operator.
     *
     * Join predicates are stored as elements of `this.joinPredicates`.
     *
     * @param prevMap  maps a set of tables to a query operator over the set of
     *                 tables. Each set should have pass number - 1 elements.
     * @param pass1Map each set contains exactly one table maps to a single
     *                 table access (scan) query operator.
     * @return a mapping of table names to a join QueryOperator. The number of
     * elements in each set of table names should be equal to the pass number.
     */
    public Map<Set<String>, QueryOperator> minCostJoins(
            Map<Set<String>, QueryOperator> prevMap,
            Map<Set<String>, QueryOperator> pass1Map) {
        Map<Set<String>, QueryOperator> result = new HashMap<>();
        // TODO(proj3_part2): implement
        // We provide a basic description of the logic you have to implement:
        // For each set of tables in prevMap
        //   For each join predicate listed in this.joinPredicates
        //      Get the left side and the right side of the predicate (table name and column)
        //
        //      Case 1: The set contains left table but not right, use pass1Map
        //              to fetch an operator to access the rightTable
        //      Case 2: The set contains right table but not left, use pass1Map
        //              to fetch an operator to access the leftTable.
        //      Case 3: Otherwise, skip this join predicate and continue the loop.
        //
        //      Using the operator from Case 1 or 2, use minCostJoinType to
        //      calculate the cheapest join with the new table (the one you
        //      fetched an operator for from pass1Map) and the previously joined
        //      tables. Then, update the result map if needed.

        for (Map.Entry<Set<String>, QueryOperator> prevElement : prevMap.entrySet()) {
            Set<String> prevSet = prevElement.getKey();
            QueryOperator prevOp = prevElement.getValue();
            for (JoinPredicate joinPredicate : this.joinPredicates) {
                String leftTable = joinPredicate.leftTable;
                String rightTable = joinPredicate.rightTable;
                if (!(prevSet.contains(leftTable) ^ prevSet.contains(rightTable))) {
                    continue;
                }
                Set<String> nextSet;
                QueryOperator newOp;
                if (prevSet.contains(leftTable)) {
                    nextSet = new HashSet<>(prevSet);
                    nextSet.add(rightTable);
                    newOp = pass1Map.get(new HashSet<String>() {{
                        add(rightTable);
                    }});
                    QueryOperator nextOp = minCostJoinType(prevOp, newOp, joinPredicate.leftColumn, joinPredicate.rightColumn);
                    if (!result.containsKey(nextSet)
                            || result.get(nextSet).estimateIOCost() > nextOp.estimateIOCost()) {
                        result.put(nextSet, nextOp);
                    }
                } else {
                    nextSet = new HashSet<>(prevSet);
                    nextSet.add(leftTable);
                    newOp = pass1Map.get(new HashSet<String>() {{
                        add(leftTable);
                    }});
                    QueryOperator nextOp = minCostJoinType(prevOp, newOp, joinPredicate.rightColumn, joinPredicate.leftColumn);
                    if (!result.containsKey(nextSet) || result.get(nextSet).estimateIOCost() > nextOp.estimateIOCost()) {
                        result.put(nextSet, nextOp);
                    }
                }

            }
        }
        return result;
    }

    // Task 7: Optimal Plan Selection //////////////////////////////////////////

    /**
     * 返回map中开销最小的那个QueryOperator
     * <br>
     * Finds the lowest cost QueryOperator in the given mapping. A mapping is
     * generated on each pass of the search algorithm, and relates a set of tables
     * to the lowest cost QueryOperator accessing those tables.
     *
     * @return a QueryOperator in the given mapping
     */
    private QueryOperator minCostOperator(Map<Set<String>, QueryOperator> map) {
        if (map.size() == 0) throw new IllegalArgumentException(
                "Can't find min cost operator over empty map"
        );
        QueryOperator minOp = null;
        int minCost = Integer.MAX_VALUE;
        for (Set<String> tables : map.keySet()) {
            QueryOperator currOp = map.get(tables);
            int currCost = currOp.estimateIOCost();
            if (currCost < minCost) {
                minOp = currOp;
                minCost = currCost;
            }
        }
        return minOp;
    }

    /**
     * Generates an optimized QueryPlan based on the System R cost-based query
     * optimizer.
     *
     * @return an iterator of records that is the result of this query
     */
    public Iterator<Record> execute() {
        this.transaction.setAliasMap(this.aliases);
        // TODO(proj3_part2): implement
        // Pass 1: For each table, find the lowest cost QueryOperator to access
        // the table. Construct a mapping of each table name to its lowest cost
        // operator.
        //
        // Pass i: On each pass, use the results from the previous pass to find
        // the lowest cost joins with each table from pass 1. Repeat until all
        // tables have been joined.
        //
        // Set the final operator to the lowest cost operator from the last
        // pass, add group by, project, sort and limit operators, and return an
        // iterator over the final operator.

        //pass 1
        Map<Set<String>, QueryOperator> pass1Map = new HashMap<>();
        for (String table : this.tableNames) {
            QueryOperator op = minCostSingleAccess(table);
            pass1Map.put(new HashSet<String>() {{
                add(table);
            }}, op);
        }
        //pass i
        Map<Set<String>, QueryOperator> result = pass1Map;
        for (int i = 1; i < this.tableNames.size(); i++) {
            result = minCostJoins(result, pass1Map);
        }
        this.finalOperator = minCostOperator(result);
        this.addGroupBy();
        this.addProject();
        this.addSort();
        this.addLimit();
        return this.finalOperator.iterator();
//        return this.executeNaive(); // TODO(proj3_part2): Replace this!
    }

    // EXECUTE NAIVE ///////////////////////////////////////////////////////////
    // The following functions are used to generate a naive query plan. You're
    // free to look to them for guidance, but you shouldn't need to use any of
    // these methods when you implement your own execute function.

    /**
     * Given a simple query over a single table without any joins, such as:
     *      SELECT * FROM table WHERE table.column >= 186;
     *
     * We can take advantage of an index over table.column to perform a over
     * only values that meet the predicate. This function determines whether or
     * not there are any columns that we can perform this optimization with.
     *
     * @return -1 if no eligible select predicate is found, otherwise the index
     * of the eligible select predicate.
     */
    private int getEligibleIndexColumnNaive() {
        boolean hasGroupBy = this.groupByColumns.size() > 0;
        boolean hasJoin = this.joinPredicates.size() > 0;
        if (hasGroupBy || hasJoin) return -1;
        for (int i = 0; i < selectPredicates.size(); i++) {
            // For each selection predicate, check if we have an index on the
            // predicate's column. If the predicate operator is something
            // we can perform a scan with (=, >=, >, <=, <) then return
            // the index of the eligible predicate
            SelectPredicate predicate = selectPredicates.get(i);
            boolean hasIndex = this.transaction.indexExists(
                    this.tableNames.get(0), predicate.column
            );
            if (hasIndex && predicate.operator != PredicateOperator.NOT_EQUALS) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Generates a query plan over a single table that takes advantage of an
     * index over the column of `indexPredicate`.
     *
     * @param indexPredicate The index of the select predicate which we can use
     *                       in our index scan.
     */
    private void generateIndexPlanNaive(int indexPredicate) {
        SelectPredicate predicate = this.selectPredicates.get(indexPredicate);
        this.finalOperator = new IndexScanOperator(
                this.transaction, this.tableNames.get(0),
                predicate.column,
                predicate.operator,
                predicate.value
        );
        this.selectPredicates.remove(indexPredicate);
        this.addSelectsNaive();
        this.addProject();
    }

    /**
     * 原生的实现，未采取任何优化
     * <>br</>
     * Generates a naive QueryPlan in which all joins are at the bottom of the
     * DAG followed by all select predicates, an optional group by operator, an
     * optional project operator, an optional sort operator, and an optional
     * limit operator (in that order).
     *
     * @return an iterator of records that is the result of this query
     */
    public Iterator<Record> executeNaive() {
        this.transaction.setAliasMap(this.aliases);
        int indexPredicate = this.getEligibleIndexColumnNaive();
        if (indexPredicate != -1) {
            this.generateIndexPlanNaive(indexPredicate);
        } else {
            // start off with a scan on the first table
            //初始化finalOperator为整张表的所有记录
            this.finalOperator = new SequentialScanOperator(
                    this.transaction,
                    this.tableNames.get(0)
            );

            // add joins, selects, group by's and projects to our plan
            // 在这些方法中不断地构建运算符树并更新finalOperator为树根
            this.addJoinsNaive();
            this.addSelectsNaive();
            this.addGroupBy();
            this.addProject();
            this.addSort();
            this.addLimit();
        }
        return this.finalOperator.iterator();
    }

}
