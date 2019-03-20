package com.aadebnuget.app;


import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Programs;
import org.apache.commons.lang3.tuple.Triple;


/*
 * public class Triple {
    public String s;
    public String p;
    public String o;

    public Triple(String s, String p, String o) {
        super();
        this.s = s;
        this.p = p;
        this.o = o;
    }

}
*/
 
/**
 * Unit test for simple App.
 */
public class OneTest 
{
	public static class TestSchema {
		// public final Triple rdf = Triple.of("s", "p", "o");
        public final Triple[] rdf = { Triple.of("s", "p", "o")};
    }

public static Frameworks.ConfigBuilder config() {
		
		
		String json = "[{\"CUST_ID\":{\"a\":1},\"PROD_ID\":220,\"USER_ID\":300,\"USER_NAME\":\"user1\"},"
				+ "{\"USER_ID\":310,\"CUST_ID\":{\"a\":2},\"PROD_ID\":210,\"USER_NAME\":\"user2\"}]";
		String jsona = "[{\"DEPTNO\":{\"a\":1},\"PROD_ID\":220,\"USER_ID\":300,\"USER_NAME\":\"user1\"},"
				+ "{\"USER_ID\":310,\"DEPTNO\":{\"a\":2},\"PROD_ID\":210,\"USER_NAME\":\"user2\"}]";
	    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
	    JsonSchema jschema = new JsonSchema("EMP", json);
	    // bad 
	    //    SchemaPlus splus= rootSchema.add("SCOTT", new JsonSchema("EMP", json)).add("SCOTT", new JsonSchema("DEPT", jsona));
	    SchemaPlus splus= rootSchema.add("SCOTT", new JsonSchema("EMP", json));
	    
	    Table newtable= jschema.getTable("EMP");
	    System.out.println("newTable="+newtable);
	    Table newtable1= jschema.getTable("EMP1");
	    System.out.println("newTable="+newtable1);
	    Table newtable2= rootSchema.getTable("EMP");
	    System.out.println("newTable="+newtable2);
	    Table newtable3= rootSchema.getTable("SCOTT.EMP");
	    System.out.println("newTable3="+newtable3);
	    System.out.println("gettablenames="+rootSchema.getTableNames());
	    System.out.println("splus gettablenames="+splus.getTableNames());
	    return Frameworks.newConfigBuilder()
	        .parserConfig(SqlParser.Config.DEFAULT)
	        .defaultSchema(splus)
	     //   .defaultSchema(
	     //       CalciteAssert.addSchema(rootSchema, CalciteAssert.SchemaSpec.SCOTT))
	        .traitDefs((List<RelTraitDef>) null)
	        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2));
	        
		// FrameworkConfig calciteFrameworkConfig =
			//	 Frameworks.newConfigBuilder().parserConfig(SqlParser.configBuilder().setLex(Lex.MYSQL).build());
	  }
	
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue()
    {
    	/*
    	//Triple<String, String, String> aa=
    	//		Triple<String, String, String>.of("s", "p", "o");
    	final Triple<String, String, String> triple = Triple.of("aa", "foo", "aa");
    	final Triple<String, String, String>[] rdf = 
    			{ Triple.of("aa", "foo", "aa") };
    			*/
		String json = "[{\"CUST_ID\":{\"a\":1},\"PROD_ID\":220,\"USER_ID\":300,\"USER_NAME\":\"user1\"},"
				+ "{\"USER_ID\":310,\"CUST_ID\":{\"a\":2},\"PROD_ID\":210,\"USER_NAME\":\"user2\"}]";
		
    	  SchemaPlus schemaPlus = Frameworks.createRootSchema(true);
          
          //给schema T中添加表
     //     schemaPlus.add("T", new ReflectiveSchema(new TestSchema()));
    	  SchemaPlus splus= schemaPlus.add("SCOTT", new JsonSchema("EMP", json));
    	  
          Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder();
          //设置默认schema
          configBuilder.defaultSchema(splus);

          FrameworkConfig frameworkConfig = configBuilder.build();

          SqlParser.ConfigBuilder paresrConfig = SqlParser.configBuilder(frameworkConfig.getParserConfig());
          
          //SQL 大小写不敏感
          paresrConfig.setCaseSensitive(false).setConfig(paresrConfig.build());

          Planner planner = Frameworks.getPlanner(frameworkConfig);

          SqlNode sqlNode;
          RelRoot relRoot = null;
          try {
              //parser阶段
           //   sqlNode = planner.parse("select \"a\".\"s\", count(\"a\".\"s\") from \"T\".\"rdf\" \"a\" group by \"a\".\"s\"");
        	  sqlNode = planner.parse("select * from EMP");
        	  //validate阶段
              planner.validate(sqlNode);
              //获取RelNode树的根
              relRoot = planner.rel(sqlNode);
          } catch (Exception e) {
              e.printStackTrace();
          }

          RelNode relNode = relRoot.project();
          System.out.print(RelOptUtil.toString(relNode));
    	}
    
   
}
