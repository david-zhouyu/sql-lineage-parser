package com.daoc.lineage.utils;

import com.daoc.lineage.engine.LineageParserEngine;
import com.daoc.lineage.common.DBType;
import org.springframework.util.ResourceUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class DruidTestComplex {
    String file = this.getClass().getResource("/sql2").getFile();

    public static void main(String[] args) throws IOException {

        File file = ResourceUtils.getFile(new DruidTestComplex().file);
        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
        String line = "";
        StringBuilder sb = new StringBuilder();
        while ((line = bufferedReader.readLine()) != null) {
            sb.append(line);
        }
        bufferedReader.close();

       LineageParserEngine parserEngine = new LineageParserEngine(sb.toString(), DBType.MYSQL.name());
       parserEngine.setDebug(true);
       parserEngine.generatorLineage();

    }
}
