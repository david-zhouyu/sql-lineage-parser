package com.daoc.lineage.utils;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.daoc.lineage.common.LineageColumn;
import com.daoc.lineage.common.TreeNode;


import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;


public class LineageUtils {

    public static TreeNode<LineageColumn> CompColumnLineageResult(TreeNode<LineageColumn> insertColumnTreeNode, TreeNode<LineageColumn> selectColumnTreeNode){
        List<TreeNode<LineageColumn>> insertTreeNodeList = insertColumnTreeNode.getChildren();
        List<TreeNode<LineageColumn>> selectTreeNodeList = selectColumnTreeNode.getChildren();
        LineageColumn rootLineageColumn = new LineageColumn();
        TreeNode<LineageColumn> rootNode = new TreeNode<>(rootLineageColumn);
        for (TreeNode<LineageColumn> treeNode : insertTreeNodeList) {
            rootNode.addChild(treeNode);
            System.out.println(treeNode.getData().toString());
            int i = 0;
            for (TreeNode<LineageColumn> columnTreeNode : selectTreeNodeList) {
                System.out.println(i++ +":" + columnTreeNode.getData().toString());

                if(columnTreeNode.getData().getTargetColumnName().equals(treeNode.getData().getTargetColumnName())
                        && columnTreeNode.getData().getExpression().equals(treeNode.getData().getExpression())){
                    treeNode.addChild(columnTreeNode);
                    System.out.println("***");
                    System.out.println();
                }
            }
        }
        return rootNode;
    }


    public static void selectColumnLineageAnalyzer(String sql,TreeNode<LineageColumn> node,String dbName) {
        AtomicReference<Boolean> isContinue = new AtomicReference<>(false);
        List<SQLStatement> sqlStatements = SQLUtils.parseStatements(sql, dbName);
        SQLStatement statement = sqlStatements.get(0);
        if(statement instanceof SQLSelectStatement) {
            // 只考虑查询语句
            SQLSelectStatement sqlSelectStatement = (SQLSelectStatement) statement;
            SQLSelectQuery sqlSelectQuery = sqlSelectStatement.getSelect().getQuery();
            // 非union的查询语句
            if (sqlSelectQuery instanceof SQLSelectQueryBlock) {
                SQLSelectQueryBlock sqlSelectQueryBlock = (SQLSelectQueryBlock) sqlSelectQuery;
                // 获取字段列表
                List<SQLSelectItem> selectItems = sqlSelectQueryBlock.getSelectList();
                selectItems.forEach(x -> {
                    // 处理---------------------
                    String column = EmptyUtils.isEmpty(x.getAlias()) ? x.toString() : x.getAlias();

                    if (column.contains(".")) {
                        column = column.substring(column.indexOf(".") + 1);
                    }
                    column = column.replace("`", "");

                    String expr = x.getExpr().toString();
                    LineageColumn myColumn = new LineageColumn();
                    myColumn.setTargetColumnName(column);
                    myColumn.setExpression(expr);
                    setColumnType(x.getExpr(), myColumn);
                    TreeNode<LineageColumn> itemNode = new TreeNode<>(myColumn);
                    SQLExpr expr1 = x.getExpr();
                    //解析表达式，添加解析结果子节点
                    handlerExpr(expr1, itemNode);

                    if (node.getLevel() == 0 || node.getData().getTargetColumnName().equals(column) || "*".equals(node.getData().getTargetColumnName())) {
                        node.addChild(itemNode);
                        isContinue.set(true);
                    }

                });
                if (isContinue.get()) {
                    // 获取表
                    SQLTableSource table = sqlSelectQueryBlock.getFrom();
                    // 普通单表
                    if (table instanceof SQLExprTableSource) {
                        // 处理最终表---------------------
                        handlerSQLExprTableSource(node, (SQLExprTableSource) table);

                    } else if (table instanceof SQLJoinTableSource) {
                        //处理join
                        handlerSQLJoinTableSource(node, (SQLJoinTableSource) table,dbName);

                    } else if (table instanceof SQLSubqueryTableSource) {
                        // 处理 subquery ---------------------
                        handlerSQLSubqueryTableSource(node, table,dbName);

                    } else if (table instanceof SQLUnionQueryTableSource) {
                        // 处理 union ---------------------
                        handlerSQLUnionQueryTableSource(node, (SQLUnionQueryTableSource) table,dbName);
                    }
                }
                // 处理---------------------
                // union的查询语句
            }

            if (sqlSelectQuery instanceof SQLUnionQuery) {
                // 处理---------------------
                selectColumnLineageAnalyzer(((SQLUnionQuery) sqlSelectQuery).getLeft().toString(), node,dbName);
                selectColumnLineageAnalyzer(((SQLUnionQuery) sqlSelectQuery).getRight().toString(), node,dbName);
            }
        }
    }

    private static void setColumnType(SQLExpr x, LineageColumn myColumn) {
        if(x instanceof SQLIdentifierExpr) {
            myColumn.setType("SQLIdentifierExpr");
        }else if(x instanceof SQLPropertyExpr){
            myColumn.setType("SQLPropertyExpr");
        }else if(x instanceof SQLAggregateExpr){
            myColumn.setType("SQLAggregateExpr");
        }else if(x instanceof SQLCharExpr){
            myColumn.setType("SQLCharExpr");
        }else if(x instanceof SQLNCharExpr){
            myColumn.setType("SQLNCharExpr");
        }else if(x instanceof SQLNotExpr){
            myColumn.setType("SQLNotExpr");
        }else if(x instanceof SQLNullExpr){
            myColumn.setType("SQLNullExpr");
        }else if(x instanceof SQLNumberExpr){
            myColumn.setType("SQLNumberExpr");
        }else if(x instanceof SQLIntegerExpr){
            myColumn.setType("SQLIntegerExpr");
        }else if(x instanceof SQLMethodInvokeExpr){
            myColumn.setType("SQLMethodInvokeExpr");
        }else if(x  instanceof SQLInSubQueryExpr){
            myColumn.setType("SQLInSubQueryExpr");
        }else if(x  instanceof SQLSequenceExpr){
            myColumn.setType("SQLSequenceExpr");
        }else if(x instanceof SQLCaseExpr){
            myColumn.setType("SQLCaseExpr");
        }else if(x  instanceof SQLAllColumnExpr){
            myColumn.setType("SQLAllColumnExpr");
        }else {
            myColumn.setType("other");
        }


    }

    public static void insertColumnLineageAnalyzer(String sql,TreeNode<LineageColumn> node,String dbName){
        List<SQLStatement> sqlStatements = SQLUtils.parseStatements(sql, dbName);
        SQLStatement statement = sqlStatements.get(0);
        AtomicReference<Boolean> isContinue = new AtomicReference<>(false);
        if (statement instanceof SQLInsertStatement) {
            SQLInsertStatement sqlInsertStatement = (SQLInsertStatement) statement;
            String schemaName = sqlInsertStatement.getTableSource().getSchema();
            String tableName = sqlInsertStatement.getTableSource().getName().getSimpleName();
            List<SQLExpr> sqlExprs = sqlInsertStatement.getColumns();
            //insert into select 一张表
            if(sqlInsertStatement.getQuery().getQuery() instanceof SQLBlockStatement) {
                SQLSelectQueryBlock firstQueryBlock = sqlInsertStatement.getQuery().getQueryBlock();
                buildInsertColumn(node, schemaName, tableName, sqlExprs, firstQueryBlock);
            }
            //insert into select union select
            if(sqlInsertStatement.getQuery().getQuery() instanceof SQLUnionQuery){
                handlerInsertUnionSelect(schemaName,tableName,sqlExprs,((SQLUnionQuery) sqlInsertStatement.getQuery().getQuery()).getLeft(),node);
                handlerInsertUnionSelect(schemaName,tableName,sqlExprs,((SQLUnionQuery) sqlInsertStatement.getQuery().getQuery()).getRight(),node);
            }
            if(sqlInsertStatement.getQuery().getQuery() instanceof SQLSelectQueryBlock) {
                SQLSelectQueryBlock query = (SQLSelectQueryBlock) sqlInsertStatement.getQuery().getQuery();
                buildInsertColumn(node, schemaName, tableName, sqlExprs, query);
            }
        }

    }

    private static void buildInsertColumn(TreeNode<LineageColumn> node, String schemaName, String tableName, List<SQLExpr> sqlExprs, SQLSelectQueryBlock firstQueryBlock) {
        List<SQLSelectItem> selectList = firstQueryBlock.getSelectList();
        //insert into table () select xxx;
        if (sqlExprs.size() != 0 && selectList.size() != 0 && sqlExprs.size() == selectList.size()) {
            // 字段双杀，按照insert指定字段进行对应那么直接对应
            for (int i = 0; i < sqlExprs.size(); i++) {
                SQLExpr x = sqlExprs.get(i);
                SQLIdentifierExpr columExpr = (SQLIdentifierExpr) x;
                String column = columExpr.getSimpleName();
                column = column.replace("`", "");

                SQLSelectItem selectItem = selectList.get(i);
                String expr = selectItem.getExpr().toString();
                LineageColumn myColumn = new LineageColumn();
                myColumn.setTargetColumnName(column);
                myColumn.setExpression(expr);
                myColumn.setTargetSchemaName(schemaName);
                myColumn.setTargetTableName(tableName);
                setColumnType(selectItem.getExpr(), myColumn);
                TreeNode<LineageColumn> itemNode = new TreeNode<>(myColumn);
                SQLExpr expr1 = selectItem.getExpr();
                //解析表达式，添加解析结果子节点
                //handlerExpr(expr1, itemNode);

                if (node.getLevel() == 0 || node.getData().getTargetColumnName().equals(column)) {
                    node.addChild(itemNode);
                }
            }
        } else {
            //处理insert into table select ......
            if (sqlExprs.size() == 0) {
                selectList.forEach(x -> {
                    // 处理---------------------
                    String column = EmptyUtils.isEmpty(x.getAlias()) ? x.toString() : x.getAlias();
                    if (column.contains(".")) {
                        column = column.substring(column.indexOf(".") + 1);
                    }
                    column = column.replace("`", "");
                    String expr = x.getExpr().toString();
                    LineageColumn myColumn = new LineageColumn();
                    myColumn.setTargetColumnName(column);
                    myColumn.setExpression(expr);
                    myColumn.setTargetSchemaName(schemaName);
                    myColumn.setTargetTableName(tableName);
                    setColumnType(x.getExpr(), myColumn);
                    TreeNode<LineageColumn> itemNode = new TreeNode<>(myColumn);
                    //SQLExpr expr1 = x.getExpr();
                    //解析表达式，添加解析结果子节点
                   // handlerExpr(expr1, itemNode);
                    //增加判断是否存在带*
                    boolean isExists = false;
                    for (TreeNode<LineageColumn> child : node.children) {
                        if(child.getData().equals(itemNode.getData())){
                            isExists = true;
                        }
                    }
                    if((node.getChildren() == null || node.getChildren().size() == 0) || !isExists){
                        if (node.getLevel() == 0 || node.getData().getTargetColumnName().equals(column)) {
                            node.addChild(itemNode);
                        }
                    }
                });
            } else if (selectList.size() != 0 && selectList.size() == 1) {
                //处理insert () select * 的情况
                sqlExprs.forEach(sqlExpr -> {
                    // 处理---------------------
                    SQLIdentifierExpr sqlIdentifierExpr = (SQLIdentifierExpr) sqlExpr;
                    String column = sqlIdentifierExpr.getSimpleName();
                    if (column.contains(".")) {
                        column = column.substring(column.indexOf(".") + 1);
                    }
                    column = column.replace("`", "");
                    String expr = selectList.get(1).getExpr().toString();
                    LineageColumn myColumn = new LineageColumn();
                    myColumn.setTargetColumnName(column);
                    myColumn.setSourceColumnName(column);
                    myColumn.setExpression(expr);
                    myColumn.setTargetSchemaName(schemaName);
                    myColumn.setTargetTableName(tableName);
                    setColumnType(sqlExpr, myColumn);
                    TreeNode<LineageColumn> itemNode = new TreeNode<>(myColumn);
//                    SQLExpr expr1 = selectList.get(1).getExpr();
//                    //解析表达式，添加解析结果子节点
//                    handlerExpr(expr1, itemNode);
                    if (node.getLevel() == 0 || node.getData().getTargetColumnName().equals(column)) {
                        node.addChild(itemNode);
                    }
                });
            }

        }
    }

    private static void handlerInsertUnionSelect(String schemaName,String tableName,List<SQLExpr> sqlExprs,SQLSelectQuery statement,TreeNode<LineageColumn> node){
        if(statement instanceof SQLUnionQuery){
            handlerInsertUnionSelect(schemaName,tableName,sqlExprs,((SQLUnionQuery) statement).getLeft(),node);
            handlerInsertUnionSelect(schemaName,tableName,sqlExprs,((SQLUnionQuery) statement).getRight(),node);
        }else {
            if (statement instanceof SQLSelectQuery) {
                System.out.println(statement.toString());
                SQLSelectQueryBlock selectQueryBlock = (SQLSelectQueryBlock) statement;
                buildInsertColumn(node, schemaName, tableName, sqlExprs, selectQueryBlock);
            }
        }

    }
    /**
     * 处理UNION子句
     * @param node
     * @param table
     */
    private static void handlerSQLUnionQueryTableSource(TreeNode<LineageColumn> node, SQLUnionQueryTableSource table,String dbName) {
        node.getAllLeafs().stream().filter(e -> !e.getData().getIsEnd()).forEach(e->{
            selectColumnLineageAnalyzer(table.getUnion().toString(), e,dbName);
        });
    }

    /**
     * 处理sub子句
     * @param node
     * @param table
     */
    private static void handlerSQLSubqueryTableSource(TreeNode<LineageColumn> node, SQLTableSource table,String dbName) {
        node.getAllLeafs().stream().filter(e -> !e.getData().getIsEnd()).forEach(e->{
            if(table instanceof SQLSubqueryTableSource) {
                selectColumnLineageAnalyzer(((SQLSubqueryTableSource)table).getSelect().toString(),e,dbName);
            }else {
                selectColumnLineageAnalyzer(table.toString(), e, dbName);
            }
        });
    }


    /**
     * 处理JOIN
     * @param node
     * @param table
     */
    private static void handlerSQLJoinTableSource(TreeNode<LineageColumn> node,SQLJoinTableSource table,String dbName){
        // 处理---------------------
        // 子查询作为表
        node.getAllLeafs().stream().filter(e -> !e.getData().getIsEnd()).forEach(e->{
            if (table.getLeft() instanceof SQLJoinTableSource ){
                handlerSQLJoinTableSource(node, (SQLJoinTableSource) table.getLeft(),dbName);
            }else if (table.getLeft() instanceof  SQLExprTableSource){
                handlerSQLExprTableSource(node, (SQLExprTableSource) table.getLeft());
            }else if (table.getLeft() instanceof SQLSubqueryTableSource) {
                // 处理---------------------
                handlerSQLSubqueryTableSource(node, table.getLeft(),dbName);
            }
            else if (table.getLeft() instanceof SQLUnionQueryTableSource) {
                // 处理---------------------
                handlerSQLUnionQueryTableSource(node, (SQLUnionQueryTableSource) table.getLeft(),dbName);
            }
        });


        node.getAllLeafs().stream().filter(e -> !e.getData().getIsEnd()).forEach(e->{
            if (table.getRight() instanceof SQLJoinTableSource ){
                handlerSQLJoinTableSource(node, (SQLJoinTableSource) table.getRight(),dbName);
            }else if (table.getRight() instanceof  SQLExprTableSource){
                handlerSQLExprTableSource(node, (SQLExprTableSource) table.getRight());
            }else if (table.getRight() instanceof SQLSubqueryTableSource) {
                // 处理---------------------
                handlerSQLSubqueryTableSource(node, table.getRight(),dbName);
            }
            else if (table.getRight() instanceof SQLUnionQueryTableSource) {
                // 处理---------------------
                handlerSQLUnionQueryTableSource(node, (SQLUnionQueryTableSource) table.getRight(),dbName);
            }
        });
    }


    /**
     * 处理最终表
     * @param node
     * @param table
     */
    private static void handlerSQLExprTableSource(TreeNode<LineageColumn> node, SQLExprTableSource table) {
        SQLExprTableSource tableSource = table;
        String db = tableSource.getExpr() instanceof SQLPropertyExpr ? ((SQLPropertyExpr) tableSource.getExpr()).getOwner().toString().replace("`","") : "";
        String tableName = tableSource.getExpr() instanceof SQLPropertyExpr ? ((SQLPropertyExpr) tableSource.getExpr()).getName().replace("`","") : "";
        String alias = EmptyUtils.isNotEmpty(tableSource.getAlias()) ? tableSource.getAlias().replace("`","") : "";

        node.getChildren().forEach(e->{
            e.getChildren().forEach(f->{

                if (f.getData().getSourceTableName() == null || f.getData().getSourceTableName().equals(tableName) || f.getData().getSourceTableName().equals(alias)){
                    f.getData().setSourceTableName(tableSource.toString());
                    f.getData().setIsEnd(true);
                    f.getData().setExpression(e.getData().getExpression());
                    if (EmptyUtils.isNotEmpty(db)){
                        f.getData().setSourceSchemaName(db);
                    }
                }
            });

        });
    }

    /**
     * 处理表达式
     * @param sqlExpr
     * @param itemNode
     */
    private static void handlerExpr(SQLExpr sqlExpr,TreeNode<LineageColumn> itemNode) {
        //方法
        if (sqlExpr instanceof SQLMethodInvokeExpr){
            visitSQLMethodInvoke( (SQLMethodInvokeExpr) sqlExpr,itemNode);
        }
        //聚合
        else if (sqlExpr instanceof SQLAggregateExpr){
            visitSQLAggregateExpr((SQLAggregateExpr) sqlExpr,itemNode);
        }
        //case
        else if (sqlExpr instanceof SQLCaseExpr){
            visitSQLCaseExpr((SQLCaseExpr) sqlExpr,itemNode);
        }
        //比较
        else if (sqlExpr instanceof SQLBinaryOpExpr){
            visitSQLBinaryOpExpr((SQLBinaryOpExpr) sqlExpr,itemNode);
        }
        //表达式
        else if (sqlExpr instanceof SQLPropertyExpr){
            visitSQLPropertyExpr((SQLPropertyExpr) sqlExpr,itemNode);
        }
        //列
        else if (sqlExpr instanceof SQLIdentifierExpr){
            visitSQLIdentifierExpr((SQLIdentifierExpr) sqlExpr,itemNode);
        }
        //赋值表达式
        else if (sqlExpr instanceof SQLIntegerExpr){
            visitSQLIntegerExpr((SQLIntegerExpr) sqlExpr,itemNode);
        }
        //数字
        else if (sqlExpr instanceof SQLNumberExpr){
            visitSQLNumberExpr((SQLNumberExpr) sqlExpr,itemNode);
        }
        //字符
        else if (sqlExpr instanceof SQLCharExpr){
            visitSQLCharExpr((SQLCharExpr) sqlExpr,itemNode);
        }else if(sqlExpr instanceof SQLAllColumnExpr){
            visitSQLALLColumnExpr((SQLAllColumnExpr)sqlExpr,itemNode);
        }
    }

    private static void visitSQLALLColumnExpr(SQLAllColumnExpr expr, TreeNode<LineageColumn> node) {
        LineageColumn project = new LineageColumn();
        project.setTargetColumnName(expr.toString());
        project.setExpression(expr.toString());
        setColumnType(expr,project);
        TreeNode<LineageColumn> search =  node.findChildNode(project);
        if (EmptyUtils.isEmpty(search)){
            node.addChild(project);
        }
    }


    /**
     * 方法
     * @param expr
     * @param node
     */
    public static void visitSQLMethodInvoke(SQLMethodInvokeExpr expr,TreeNode<LineageColumn> node){
        if (expr.getParameters().size() == 0){
            //计算表达式，没有更多列，结束循环
            if (node.getData().getExpression().equals(expr.toString())){
                node.getData().setIsEnd(true);
            }
        }else {
            expr.getParameters().forEach( expr1 -> {
                handlerExpr(expr1,node);
            });
        }
    }


    /**
     * 聚合
     * @param expr
     * @param node
     */
    public static void visitSQLAggregateExpr(SQLAggregateExpr expr,TreeNode<LineageColumn> node){
        expr.getArguments().forEach( expr1 -> {
            handlerExpr(expr1,node);
        });
    }


    /**
     * 选择
     * @param expr
     * @param node
     */
    public static void visitSQLCaseExpr(SQLCaseExpr expr,TreeNode<LineageColumn> node){
        expr.getItems().forEach( expr1 -> {
            handlerExpr(expr1.getConditionExpr(),node);
            handlerExpr(expr1.getValueExpr(),node);
        });
        if(expr.getElseExpr() != null){
            handlerExpr(expr.getElseExpr(),node);
        }
    }


    /**
     * 判断
     * @param expr
     * @param node
     */
    public static void visitSQLBinaryOpExpr(SQLBinaryOpExpr expr,TreeNode<LineageColumn> node){
        handlerExpr(expr.getLeft(),node);
        handlerExpr(expr.getRight(),node);
    }




    /**
     * 表达式列
     * @param expr
     * @param node
     */
    public static void visitSQLPropertyExpr(SQLPropertyExpr expr,TreeNode<LineageColumn>  node){
        LineageColumn project = new LineageColumn();
        String columnName = expr.getName().replace("`","");
        project.setTargetColumnName(columnName);
        setColumnType(expr,project);
        project.setSourceTableName(expr.getOwner().toString());
        TreeNode<LineageColumn> search =  node.findChildNode(project);

        if (EmptyUtils.isEmpty(search)){
            node.addChild(project);
        }
    }

    /**
     * 列
     * @param expr
     * @param node
     */
    public static void visitSQLIdentifierExpr(SQLIdentifierExpr expr,TreeNode<LineageColumn>  node){
        LineageColumn project = new LineageColumn();
        project.setTargetColumnName(expr.getName());
        project.setExpression(expr.toString());
        setColumnType(expr,project);
        TreeNode<LineageColumn> search =  node.findChildNode(project);
        if (EmptyUtils.isEmpty(search)){
            node.addChild(project);
        }
    }


    /**
     * 整型赋值
     * @param expr
     * @param node
     */
    public static void visitSQLIntegerExpr(SQLIntegerExpr expr,TreeNode<LineageColumn>  node){
        LineageColumn project = new LineageColumn();
        project.setTargetColumnName(expr.getNumber().toString());
        //常量不设置表信息
        project.setSourceTableName("");
        project.setIsEnd(true);
        setColumnType(expr,project);
        TreeNode<LineageColumn> search =  node.findChildNode(project);
        if (EmptyUtils.isEmpty(search)){
            node.addChild(project);
        }
    }

    /**
     * 数字
     * @param expr
     * @param node
     */
    public static void visitSQLNumberExpr(SQLNumberExpr expr, TreeNode<LineageColumn>  node){
        LineageColumn project = new LineageColumn();
        project.setTargetColumnName(expr.getNumber().toString());
        //常量不设置表信息
        project.setSourceTableName("");
        project.setIsEnd(true);
        setColumnType(expr,project);
        TreeNode<LineageColumn> search =  node.findChildNode(project);
        if (EmptyUtils.isEmpty(search)){
            node.addChild(project);
        }
    }


    /**
     * 字符
     * @param expr
     * @param node
     */
    public static void visitSQLCharExpr(SQLCharExpr expr, TreeNode<LineageColumn>  node){
        LineageColumn project = new LineageColumn();
        project.setTargetColumnName(expr.toString());
        //常量不设置表信息
        project.setSourceTableName("");
        project.setIsEnd(true);
        setColumnType(expr,project);
        TreeNode<LineageColumn> search =  node.findChildNode(project);
        if (EmptyUtils.isEmpty(search)){
            node.addChild(project);
        }
    }
}
