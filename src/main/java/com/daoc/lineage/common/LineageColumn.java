package com.daoc.lineage.common;

import com.daoc.lineage.utils.EmptyUtils;
import lombok.Data;


@Data
public class LineageColumn implements Comparable<LineageColumn>  {
    private String targetColumnName;

    private String sourceTableName;

    private String sourceSchemaName;

    private String sourceColumnName;

    private String expression;

    private Boolean isEnd = false;

    private String targetTableName;

    private String targetSchemaName;

    private String type;



    public void setSourceTableName(String sourceTableName) {
        sourceTableName = EmptyUtils.isNotEmpty(sourceTableName) ? sourceTableName.replace("`","") : sourceTableName;
        if (sourceTableName.contains(".")){
            this.sourceSchemaName = sourceTableName.substring(0,sourceTableName.indexOf("."));
            this.sourceTableName = sourceTableName.substring(sourceTableName.indexOf(".")+1);
        }else {
            this.sourceTableName = sourceTableName;

        }
    }


    @Override
    public int compareTo(LineageColumn o) {
        if (this.getTargetColumnName().equals(o.getTargetColumnName())) {
            return 0;
        }
        return -1;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        LineageColumn myColumn = (LineageColumn) o;

        if (!this.getTargetColumnName().equals(myColumn.getTargetColumnName())){
            return false;
        }
        if (EmptyUtils.isNotEmpty(sourceTableName) && !sourceTableName.equals(myColumn.sourceTableName)) {
            return false;
        }
        if (EmptyUtils.isNotEmpty(sourceColumnName) && !sourceColumnName.equals(myColumn.sourceColumnName)) {
            return false;
        }

        if (EmptyUtils.isNotEmpty(expression) && !expression.equals(myColumn.expression)) {
            return false;
        }

        if (EmptyUtils.isNotEmpty(sourceSchemaName) && !sourceSchemaName.equals(myColumn.sourceSchemaName)) {
            return false;
        }

        if (EmptyUtils.isNotEmpty(targetSchemaName) && !targetSchemaName.equals(myColumn.targetSchemaName)) {
            return false;
        }

        if (EmptyUtils.isNotEmpty(targetTableName) && !targetTableName.equals(myColumn.targetTableName)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = getTargetColumnName().hashCode();
        if (EmptyUtils.isNotEmpty(sourceTableName)){
            result = 31 * result + sourceTableName.hashCode();
        }
        if (EmptyUtils.isNotEmpty(sourceColumnName)){
            result = 31 * result + sourceColumnName.hashCode();
        }
        return result;
    }
}
