package com.daoc.lineage.common;

import lombok.Data;

@Data
public class LineageRelation {
    private String sourceTable;
    private String sourceColumn;
    private String sourceSchema;
    private String targetTable;
    private String targetColumn;
    private String targetSchema;
    private String type;
}
