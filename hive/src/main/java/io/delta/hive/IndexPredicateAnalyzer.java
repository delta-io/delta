/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * This file contains code from the Apache Spark project (original license above).
 * It contains modifications, which are licensed as follows:
 */

/*
 * Copyright (2020) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.hive;

import java.util.*;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.lib.*;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.*;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copy from Hive org.apache.hadoop.hive.ql.index.IndexPredicateAnalyzer
 * IndexPredicateAnalyzer decomposes predicates, separating the parts
 * which can be satisfied by an index from the parts which cannot.
 * Currently, it only supports pure conjunctions over binary expressions
 * comparing a column reference with a constant value.  It is assumed
 * that all column aliases encountered refer to the same table.
 */
public class IndexPredicateAnalyzer {

    private static final Logger LOG = LoggerFactory.getLogger(IndexPredicateAnalyzer.class);

    private final Set<String> udfNames;
    private final Map<String, Set<String>> columnToUDFs;

    public IndexPredicateAnalyzer() {
        udfNames = new HashSet<String>();
        columnToUDFs = new HashMap<String, Set<String>>();
    }

    /**
     * Registers a comparison operator as one which can be satisfied
     * by an index search.  Unless this is called, analyzePredicate
     * will never find any indexable conditions.
     *
     * @param udfName name of comparison operator as returned
     * by either {@link GenericUDFBridge#getUdfName} (for simple UDF's)
     * or udf.getClass().getName() (for generic UDF's).
     */
    public void addComparisonOp(String udfName) {
        udfNames.add(udfName);
    }

    /**
     * Clears the set of column names allowed in comparisons.  (Initially, all
     * column names are allowed.)
     */
    public void clearAllowedColumnNames() {
        columnToUDFs.clear();
    }

    /**
     * Adds a column name to the set of column names allowed.
     *
     * @param columnName name of column to be allowed
     */
    public void allowColumnName(String columnName) {
        columnToUDFs.put(columnName, udfNames);
    }

    /**
     * add allowed functions per column
     * @param columnName
     * @param udfs
     */
    public void addComparisonOp(String columnName, String... udfs) {
        Set<String> allowed = columnToUDFs.get(columnName);
        if (allowed == null || allowed == udfNames) {
            // override
            columnToUDFs.put(columnName, new HashSet<String>(Arrays.asList(udfs)));
        } else {
            allowed.addAll(Arrays.asList(udfs));
        }
    }

    /**
     * Analyzes a predicate.
     *
     * @param predicate predicate to be analyzed
     *
     * @param searchConditions receives conditions produced by analysis
     *
     * @return residual predicate which could not be translated to
     * searchConditions
     */
    public ExprNodeDesc analyzePredicate(
            ExprNodeDesc predicate,
            final List<IndexSearchCondition> searchConditions) {

        Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
        NodeProcessor nodeProcessor = new NodeProcessor() {
            public Object process(Node nd, Stack<Node> stack,
                                  NodeProcessorCtx procCtx, Object... nodeOutputs)
                    throws SemanticException {

                // We can only push down stuff which appears as part of
                // a pure conjunction:  reject OR, CASE, etc.
                for (Node ancestor : stack) {
                    if (nd == ancestor) {
                        break;
                    }
                    if (!FunctionRegistry.isOpAnd((ExprNodeDesc) ancestor)) {
                        return nd;
                    }
                }

                return analyzeExpr((ExprNodeGenericFuncDesc) nd, searchConditions, nodeOutputs);
            }
        };

        Dispatcher disp = new DefaultRuleDispatcher(
                nodeProcessor, opRules, null);
        GraphWalker ogw = new DefaultGraphWalker(disp);
        ArrayList<Node> topNodes = new ArrayList<Node>();
        topNodes.add(predicate);
        HashMap<Node, Object> nodeOutput = new HashMap<Node, Object>();
        try {
            ogw.startWalking(topNodes, nodeOutput);
        } catch (SemanticException ex) {
            throw new RuntimeException(ex);
        }
        ExprNodeDesc residualPredicate = (ExprNodeDesc) nodeOutput.get(predicate);
        return residualPredicate;
    }

    //Check if ExprNodeColumnDesc is wrapped in expr.
    //If so, peel off. Otherwise return itself.
    private ExprNodeDesc getColumnExpr(ExprNodeDesc expr) {
        if (expr instanceof ExprNodeColumnDesc) {
            return expr;
        }
        ExprNodeGenericFuncDesc funcDesc = null;
        if (expr instanceof ExprNodeGenericFuncDesc) {
            funcDesc = (ExprNodeGenericFuncDesc) expr;
        }
        if (null == funcDesc) {
            return expr;
        }
        GenericUDF udf = funcDesc.getGenericUDF();
        // check if its a simple cast expression.
        if ((udf instanceof GenericUDFBridge || udf instanceof GenericUDFToBinary
                || udf instanceof GenericUDFToChar || udf instanceof GenericUDFToVarchar
                || udf instanceof GenericUDFToDecimal || udf instanceof GenericUDFToDate
                || udf instanceof GenericUDFToUnixTimeStamp || udf instanceof GenericUDFToUtcTimestamp)
                && funcDesc.getChildren().size() == 1
                && funcDesc.getChildren().get(0) instanceof ExprNodeColumnDesc) {
            return expr.getChildren().get(0);
        }
        return expr;
    }

    private ExprNodeDesc analyzeExpr(
            ExprNodeGenericFuncDesc expr,
            List<IndexSearchCondition> searchConditions,
            Object... nodeOutputs) throws SemanticException {

        if (FunctionRegistry.isOpAnd(expr)) {
            assert(nodeOutputs.length >= 2);
            List<ExprNodeDesc> residuals = new ArrayList<ExprNodeDesc>();
            for (Object residual : nodeOutputs) {
                if (null != residual) {
                    residuals.add((ExprNodeDesc)residual);
                }
            }
            if (residuals.size() == 0) {
                return null;
            } else if (residuals.size() == 1) {
                return residuals.get(0);
            } else {
                return new ExprNodeGenericFuncDesc(
                        TypeInfoFactory.booleanTypeInfo,
                        FunctionRegistry.getGenericUDFForAnd(),
                        residuals);
            }
        }

        GenericUDF genericUDF = expr.getGenericUDF();

        ExprNodeDesc[] peelOffExprs = new ExprNodeDesc[nodeOutputs.length];
        List<ExprNodeColumnDesc> exprNodeColDescs = new ArrayList<ExprNodeColumnDesc>();
        List<ExprNodeConstantDesc> exprConstantColDescs = new ArrayList<ExprNodeConstantDesc>();

        for (int i = 0; i < nodeOutputs.length; i++) {
            // We may need to peel off the GenericUDFBridge that is added by CBO or user
            ExprNodeDesc peelOffExpr = getColumnExpr((ExprNodeDesc)nodeOutputs[i]);
            if (peelOffExpr instanceof ExprNodeColumnDesc) {
                exprNodeColDescs.add((ExprNodeColumnDesc)peelOffExpr);
            } else if (peelOffExpr instanceof ExprNodeConstantDesc) {
                exprConstantColDescs.add((ExprNodeConstantDesc)peelOffExpr);
            }

            peelOffExprs[i] = peelOffExpr;
        }

        if (exprNodeColDescs.size() != 1) {
            if (LOG.isInfoEnabled()) {
                LOG.info("Pushed down expr should only have one column, while it is " + StringUtils.join(exprNodeColDescs.toArray()));
            }
            return expr;
        }

        ExprNodeColumnDesc columnDesc = exprNodeColDescs.get(0);

        Set<String> allowed = columnToUDFs.get(columnDesc.getColumn());
        if (allowed == null) {
            if (LOG.isInfoEnabled()) {
                LOG.info("This column " + columnDesc.getColumn() + " is not allowed to pushed down to delta...");
            }
            return expr;
        }

        String udfClassName = genericUDF.getUdfName();
        if (genericUDF instanceof GenericUDFBridge) {
            udfClassName = ((GenericUDFBridge) genericUDF).getUdfClassName();
        }
        if (!allowed.contains(udfClassName)) {
            if (LOG.isInfoEnabled()) {
                LOG.info("This udf " + genericUDF.getUdfName() + " is not allowed to pushed down to delta...");
            }
            return expr;
        }

        if (!udfClassName.equals("org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn")
                && exprConstantColDescs.size() > 1) {
            if (LOG.isInfoEnabled()) {
                LOG.info("There should be one constant in this udf(" + udfClassName + ") except UDFIn");
            }
            return expr;
        }

        // We also need to update the expr so that the index query can be generated.
        // Note that, hive does not support UDFToDouble etc in the query text.
        ExprNodeGenericFuncDesc indexExpr =
                new ExprNodeGenericFuncDesc(expr.getTypeInfo(), expr.getGenericUDF(), Arrays.asList(peelOffExprs));

        searchConditions.add(
                new IndexSearchCondition(
                        columnDesc,
                        udfClassName,
                        null,
                        indexExpr,
                        expr,
                        null));

        // we converted the expression to a search condition, so
        // remove it from the residual predicate
        return null;
    }

    /**
     * Translates search conditions back to ExprNodeDesc form (as
     * a left-deep conjunction).
     *
     * @param searchConditions (typically produced by analyzePredicate)
     *
     * @return ExprNodeGenericFuncDesc form of search conditions
     */
    public ExprNodeGenericFuncDesc translateSearchConditions(
            List<IndexSearchCondition> searchConditions) {

        ExprNodeGenericFuncDesc expr = null;
        for (IndexSearchCondition searchCondition : searchConditions) {
            if (expr == null) {
                expr = searchCondition.getIndexExpr();
                continue;
            }
            List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>();
            children.add(expr);
            children.add(searchCondition.getIndexExpr());
            expr = new ExprNodeGenericFuncDesc(
                    TypeInfoFactory.booleanTypeInfo,
                    FunctionRegistry.getGenericUDFForAnd(),
                    children);
        }
        return expr;
    }

    /**
     * Translates original conditions back to ExprNodeDesc form (as
     * a left-deep conjunction).
     *
     * @param searchConditions (typically produced by analyzePredicate)
     *
     * @return ExprNodeGenericFuncDesc form of search conditions
     */
    public ExprNodeGenericFuncDesc translateOriginalConditions(
            List<IndexSearchCondition> searchConditions) {

        ExprNodeGenericFuncDesc expr = null;
        for (IndexSearchCondition searchCondition : searchConditions) {
            if (expr == null) {
                expr = searchCondition.getOriginalExpr();
                continue;
            }
            List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>();
            children.add(expr);
            children.add(searchCondition.getOriginalExpr());
            expr = new ExprNodeGenericFuncDesc(
                    TypeInfoFactory.booleanTypeInfo,
                    FunctionRegistry.getGenericUDFForAnd(),
                    children);
        }
        return expr;
    }
}
