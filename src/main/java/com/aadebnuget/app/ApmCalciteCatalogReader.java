package com.aadebnuget.app;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.Frameworks;

//import com.github.quxiucheng.tutorial.common.data.MockData;

public class ApmCalciteCatalogReader {


    public static CalciteCatalogReader createMockCatalogReader(SqlParser.Config parserConfig) {
        SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        rootSchema.add("hr", ApmData.apmhr());
        /*
		String json = "[{\"CUST_ID\":{\"a\":1},\"PROD_ID\":220,\"USER_ID\":300,\"USER_NAME\":\"user1\"},"
				+ "{\"USER_ID\":310,\"CUST_ID\":{\"a\":2},\"PROD_ID\":210,\"USER_NAME\":\"user2\"}]";
		String jsona = "[{\"DEPTNO\":{\"a\":1},\"PROD_ID\":220,\"USER_ID\":300,\"USER_NAME\":\"user1\"},"
				+ "{\"USER_ID\":310,\"DEPTNO\":{\"a\":2},\"PROD_ID\":210,\"USER_NAME\":\"user2\"}]";
	    
	    MultiJsonSchema jschema = new MultiJsonSchema(new ArrayList<String>(Arrays.asList("EMP", "DEPT")),
	    		new ArrayList<String>(Arrays.asList(json,jsona)));
	    		*/
	  //   rootSchema.add("hr", jschema);
        return createCatalogReader(parserConfig, rootSchema);
    }

    public static CalciteCatalogReader createCatalogReader(SqlParser.Config parserConfig, SchemaPlus rootSchema) {

        Properties prop = new Properties();
        prop.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
                String.valueOf(parserConfig.caseSensitive()));
        // 设置时区
        prop.setProperty(CalciteConnectionProperty.TIME_ZONE.camelName(), "GMT+:08:00");
        CalciteConnectionConfigImpl calciteConnectionConfig = new CalciteConnectionConfigImpl(prop);
        return new CalciteCatalogReader(
                CalciteSchema.from(rootSchema),
                CalciteSchema.from(rootSchema).path(null),
                new JavaTypeFactoryImpl(),
                calciteConnectionConfig
        );
    }

}