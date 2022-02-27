package com.daoc.lineage.engine;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.fastjson.JSONObject;
import com.daoc.lineage.common.DBType;
import com.daoc.lineage.utils.EmptyUtils;
import com.daoc.lineage.common.LineageColumn;
import com.daoc.lineage.common.TreeNode;
import com.daoc.lineage.utils.LineageUtils;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class LineageParserEngine {
    private String dbName;
    private String sql;
    private boolean debug = false;
    public LineageParserEngine(String sql, String dbType){
            this.dbName = DBType.getDbName(dbType);
            this.sql = sql;

    }

    public void generatorLineage(){
        if ( EmptyUtils.isEmpty(sql)){
            return ;
        }
        List<SQLStatement> statements = new ArrayList<>();
        // 解析
        try{
            statements = SQLUtils.parseStatements(sql, dbName);
        }catch (Exception e){
            System.out.println("can't parser by druid MYSQL"+e);
        }
        List<TreeNode<LineageColumn>> result = new ArrayList<>();
        for (SQLStatement statement : statements) {
            if(statement instanceof SQLSelectStatement){
                sql = ((SQLSelectStatement) statement).getSelect().toString();
                LineageColumn top = new LineageColumn();
                TreeNode<LineageColumn> root = new TreeNode<>(top);
                LineageUtils.selectColumnLineageAnalyzer(sql,root,dbName);
                result.add(root);
                if(debug) {
                    printLineage(root);
                }
            }
            if(statement instanceof SQLInsertStatement){
                LineageColumn top = new LineageColumn();
                TreeNode<LineageColumn> insertRoot = new TreeNode<>(top);
                sql = ((SQLInsertStatement) statement).toString();
                LineageUtils.insertColumnLineageAnalyzer(sql,insertRoot,dbName);
                top = new LineageColumn();
                TreeNode<LineageColumn> selectRoot = new TreeNode<>(top);
                LineageUtils.selectColumnLineageAnalyzer(((SQLInsertStatement) statement).getQuery().toString(),selectRoot,dbName);
                LineageUtils.CompColumnLineageResult(insertRoot,selectRoot);
                result.add(insertRoot);
                if(debug) {
                    printLineage(insertRoot);
                }
            }
        }

    }

    private void printLineage(TreeNode<LineageColumn> root) {
        for (TreeNode<LineageColumn> node : root) {
            StringBuilder indent = new StringBuilder();
            for (int i = 1; i < node.getLevel(); i++) {
                indent.append("     ");
            }
            System.out.println(indent.toString() + JSONObject.toJSONString(node.getData()));
        }
    }
}
