package com.aadebnuget.app;


import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.config.Lex;
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
import com.aadebnuget.app.MultiJsonSchema;


public class MySqlTest {

	
public static Frameworks.ConfigBuilder config() {
		
		
		String json = "[{\"CUST_ID\":{\"a\":1},\"PROD_ID\":220,\"USER_ID\":300,\"USER_NAME\":\"user1\"},"
				+ "{\"USER_ID\":310,\"CUST_ID\":{\"a\":2},\"PROD_ID\":210,\"USER_NAME\":\"user2\"}]";
		String jsona = "[{\"DEPTNO\":{\"a\":1},\"PROD_ID\":220,\"USER_ID\":300,\"USER_NAME\":\"user1\"},"
				+ "{\"USER_ID\":310,\"DEPTNO\":{\"a\":2},\"PROD_ID\":210,\"USER_NAME\":\"user2\"}]";
	    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
	    
	    MultiJsonSchema jschema = new MultiJsonSchema(new ArrayList<String>(Arrays.asList("EMP", "DEPT")),
	    		new ArrayList<String>(Arrays.asList(json,jsona)));
	    // bad 
	    //    SchemaPlus splus= rootSchema.add("SCOTT", new JsonSchema("EMP", json)).add("SCOTT", new JsonSchema("DEPT", jsona));
	//    SchemaPlus splus= rootSchema.add("SCOTT", new JsonSchema("EMP", json));
	    SchemaPlus splus= rootSchema.add("SCOTT", jschema);
		  
	    
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
    public void sql1Test()
    {
    	/*
    	//Triple<String, String, String> aa=
    	//		Triple<String, String, String>.of("s", "p", "o");
    	final Triple<String, String, String> triple = Triple.of("aa", "foo", "aa");
    	final Triple<String, String, String>[] rdf = 
    			{ Triple.of("aa", "foo", "aa") };
    			*/
		String json = "[{\"EMP_ID\":{\"a\":1},\"ORGANIZATION\":220,\"USER_ID\":300,\"FIRST_NAME\":\"user1\"},"
				+ "{\"USER_ID\":310,\"EMP_ID\":{\"a\":2},\"PROD_ID\":210,\"FIRST_NAME\":\"user2\"}]";
		
		String jsona = "[{\"EMP_ID\":{\"a\":1},\"ORGANIZATION\":220,\"SALARY\":300,\"USER_NAME\":\"user1\"},"
				+ "{\"SALARY\":310,\"EMP_ID\":{\"a\":2},\"PROD_ID\":210,\"USER_NAME\":\"user2\"}]";	
    	  SchemaPlus schemaPlus = Frameworks.createRootSchema(true);
          
  	    MultiJsonSchema jschema = new MultiJsonSchema(new ArrayList<String>(Arrays.asList("EMPLOYEE", "SALARY")),
	    		new ArrayList<String>(Arrays.asList(json,jsona)));
  	    
  	  SchemaPlus splus= schemaPlus.add("SCOTT", jschema);
          //给schema T中添加表
     //     schemaPlus.add("T", new ReflectiveSchema(new TestSchema()));
    //	  SchemaPlus splus= schemaPlus.add("SCOTT", new JsonSchema("EMP", json));
    	  
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
        	  sqlNode = planner.parse("SELECT e.first_name AS FirstName, s.salary AS Salary from employee AS e join salary AS s on e.emp_id=s.emp_id where e.organization = 'Tesla' and s.organization = 'Tesla'");
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
    
    @Test
    public void sql2Test()
    {
    	//\"IS_BOOK_SUCCESS\":220 \"is_PAID\":1
    
    	String json = "[{\"EMP_ID\":{\"a\":1},\"IS_BOOK_SUCCESS\":220,\"DATEKEY\":300,\"FIRST_NAME\":\"user1\",\"IS_PAID\":1},"
				+ "{\"DATEKEY\":310,\"EMP_ID\":{\"a\":2},\"IS_BOOK_SUCCESS\":210,\"FIRST_NAME\":\"user2\",\"IS_PAID\":1}]";
		
		String jsona = "[{\"EMP_ID\":{\"a\":1},\"ORGANIZATION\":220,\"SALARY\":300,\"USER_NAME\":\"user1\"},"
				+ "{\"SALARY\":310,\"EMP_ID\":{\"a\":2},\"PROD_ID\":210,\"USER_NAME\":\"user2\"}]";	
    	  SchemaPlus schemaPlus = Frameworks.createRootSchema(true);
          
  	    MultiJsonSchema jschema = new MultiJsonSchema(new ArrayList<String>(Arrays.asList("MYORDER", "SALARY")),
	    		new ArrayList<String>(Arrays.asList(json,jsona)));
  	    
  	  SchemaPlus splus= schemaPlus.add("MYSQL", jschema);
          //给schema T中添加表
     //     schemaPlus.add("T", new ReflectiveSchema(new TestSchema()));
    //	  SchemaPlus splus= schemaPlus.add("SCOTT", new JsonSchema("EMP", json));
    	  
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
	    	String yourQuery=
	    			"SELECT datekey FROM myorder WHERE is_paid = 1 and datekey = 20190210\n";
          try {
              //parser阶段
           //   sqlNode = planner.parse("select \"a\".\"s\", count(\"a\".\"s\") from \"T\".\"rdf\" \"a\" group by \"a\".\"s\"");
        	  sqlNode = planner.parse(yourQuery);
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
	
    
    @Test
    public void sql3Test()
    {
    
    	String json = "[{\"EMP_ID\":{\"a\":1},\"IS_BOOK_SUCCESS\":220,\"DATEKEY\":300,\"FIRST_NAME\":\"user1\"},"
				+ "{\"DATEKEY\":310,\"EMP_ID\":{\"a\":2},\"IS_BOOK_SUCCESS\":210,\"FIRST_NAME\":\"user2\"}]";
		
		String jsona = "[{\"EMP_ID\":{\"a\":1},\"ORGANIZATION\":220,\"SALARY\":300,\"USER_NAME\":\"user1\"},"
				+ "{\"SALARY\":310,\"EMP_ID\":{\"a\":2},\"PROD_ID\":210,\"USER_NAME\":\"user2\"}]";	
    	  SchemaPlus schemaPlus = Frameworks.createRootSchema(true);
          
  	    MultiJsonSchema jschema = new MultiJsonSchema(new ArrayList<String>(Arrays.asList("MYORDER", "SALARY")),
	    		new ArrayList<String>(Arrays.asList(json,jsona)));
  	    
  	  SchemaPlus splus= schemaPlus.add("MYSQL", jschema);
          //给schema T中添加表
     //     schemaPlus.add("T", new ReflectiveSchema(new TestSchema()));
    //	  SchemaPlus splus= schemaPlus.add("SCOTT", new JsonSchema("EMP", json));
    	  
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
      	String yourQuery=
  //  			"SELECT datekey,"+
      			"SELECT \n"+	    
      			"count(1) AS book_succ_cnt\n"+
  "FROM mysql.myorder\n"+
  "WHERE is_book_success = 1\n"+
    "and datekey = 20190210";
          try {
              //parser阶段
           //   sqlNode = planner.parse("select \"a\".\"s\", count(\"a\".\"s\") from \"T\".\"rdf\" \"a\" group by \"a\".\"s\"");
        	  sqlNode = planner.parse(yourQuery);
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
	
    
    @Test
    public void t1Test()
    {
    	//\"IS_BOOK_SUCCESS\":220 \"is_PAID\":1
    
    	String json = "[{\"EMP_ID\":{\"a\":1},\"IS_BOOK_SUCCESS\":220,\"DATEKEY\":300,\"FIRST_NAME\":\"user1\",\"IS_PAID\":1},"
				+ "{\"DATEKEY\":310,\"EMP_ID\":{\"a\":2},\"IS_BOOK_SUCCESS\":210,\"FIRST_NAME\":\"user2\",\"IS_PAID\":1}]";
		
		String jsona = "[{\"EMP_ID\":{\"a\":1},\"ORGANIZATION\":220,\"SALARY\":300,\"USER_NAME\":\"user1\"},"
				+ "{\"SALARY\":310,\"EMP_ID\":{\"a\":2},\"PROD_ID\":210,\"USER_NAME\":\"user2\"}]";	
    	  SchemaPlus schemaPlus = Frameworks.createRootSchema(true);
          
  	    MultiJsonSchema jschema = new MultiJsonSchema(new ArrayList<String>(Arrays.asList("MYORDER", "SALARY")),
	    		new ArrayList<String>(Arrays.asList(json,jsona)));
  	    
  	  SchemaPlus splus= schemaPlus.add("MYSQL", jschema);
          //给schema T中添加表
     //     schemaPlus.add("T", new ReflectiveSchema(new TestSchema()));
    //	  SchemaPlus splus= schemaPlus.add("SCOTT", new JsonSchema("EMP", json));
    	  
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
          System.out.println("t1=");
          final String yourQuery = 
        		  "SELECT t1.book_succ_cnt / t2.pay_order_cnt AS book_succ_rate, t2.pay_order_cnt AS pay_order_cnt FROM\n"+
        		  "( \n"+
        		  "SELECT datekey, count(1) AS book_succ_cnt FROM mysql.myorder  WHERE is_book_success = 1 and datekey = 20190210 group by datekey \n"+
        		  ") t1 \n"+
        		  "LEFT JOIN\n"+
        		  "(\n"+
        		  "SELECT datekey, count(1) AS pay_order_cnt FROM mysql.myorder WHERE is_paid = 1 and datekey = 20190210 group by datekey\n"+
        		  ") t2\n"+
        		  "ON t1.datekey = t2.datekey\n";
          try {
              //parser阶段
           //   sqlNode = planner.parse("select \"a\".\"s\", count(\"a\".\"s\") from \"T\".\"rdf\" \"a\" group by \"a\".\"s\"");
        	  sqlNode = planner.parse(yourQuery);
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
    @Test
    public void fromTest()
    {
    	//\"IS_BOOK_SUCCESS\":220 \"is_PAID\":1
    
    	String json = "[{\"EMP_ID\":{\"a\":1},\"IS_BOOK_SUCCESS\":220,\"DATEKEY\":300,\"FIRST_NAME\":\"user1\",\"IS_PAID\":1},"
				+ "{\"DATEKEY\":310,\"EMP_ID\":{\"a\":2},\"IS_BOOK_SUCCESS\":210,\"FIRST_NAME\":\"user2\",\"IS_PAID\":1}]";
		
		String jsona = "[{\"EMP_ID\":{\"a\":1},\"ORGANIZATION\":220,\"DATEKEY\":300,\"UUID\":\"user1\"},"
				+ "{\"SALARY\":310,\"EMP_ID\":{\"a\":2},\"DATEKEY\":210,\"UUID\":\"user2\"}]";	
    	  SchemaPlus schemaPlus = Frameworks.createRootSchema(true);
          
  	    MultiJsonSchema jschema = new MultiJsonSchema(new ArrayList<String>(Arrays.asList("MYORDER", "MYPAGE")),
	    		new ArrayList<String>(Arrays.asList(json,jsona)));
  	    
  	  SchemaPlus splus= schemaPlus.add("MYSQL", jschema);
          //给schema T中添加表
     //     schemaPlus.add("T", new ReflectiveSchema(new TestSchema()));
    //	  SchemaPlus splus= schemaPlus.add("SCOTT", new JsonSchema("EMP", json));
    	  
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
          System.out.println("from=");
          final String yourQuery = 
        		  "(	// 产生指标book_succ_rate(预订成功率), pay_order_cnt(支付订单数)\n"+
        		  "SELECT t1.book_succ_cnt / t2.pay_order_cnt AS book_succ_rate, t2.pay_order_cnt AS pay_order_cnt FROM\n"+
        		  "( \n"+
        		  "SELECT datekey, count(1) AS book_succ_cnt FROM mysql.myorder WHERE is_book_success = 1 and datekey = 20190210 group by datekey\n"+
        		  ") t1 \n"+
        		  "LEFT JOIN\n"+
        		  "(\n"+
        		  "SELECT datekey, count(1) AS pay_order_cnt FROM mysql.myorder WHERE is_paid = 1 and datekey = 20190210 group by datekey\n"+
        		  ") t2\n"+
        		  "ON t1.datekey = t2.datekey\n"+
        		  ") t1\n"+
        		  "LEFT JOIN \n"+
        		  "(\n"+
        		  "SELECT datekey, count(distinct uuid) AS uv FROM mysql.mypage WHERE datekey = 20190210 group by datekey\n"+
        		  ") t2\n"+
        		  "ON t11.datekey = t2.datekey";
          try {
              //parser阶段
           //   sqlNode = planner.parse("select \"a\".\"s\", count(\"a\".\"s\") from \"T\".\"rdf\" \"a\" group by \"a\".\"s\"");
        	  sqlNode = planner.parse(yourQuery);
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
    @Test
    public void rawTest()
    {
    	//\"IS_BOOK_SUCCESS\":220 \"is_PAID\":1
    
    	String json = "[{\"EMP_ID\":{\"a\":1},\"IS_BOOK_SUCCESS\":220,\"DATEKEY\":300,\"FIRST_NAME\":\"user1\",\"IS_PAID\":1},"
				+ "{\"DATEKEY\":310,\"EMP_ID\":{\"a\":2},\"IS_BOOK_SUCCESS\":210,\"FIRST_NAME\":\"user2\",\"IS_PAID\":1}]";
		
		String jsona = "[{\"EMP_ID\":{\"a\":1},\"ORGANIZATION\":220,\"DATEKEY\":300,\"UUID\":\"user1\"},"
				+ "{\"SALARY\":310,\"EMP_ID\":{\"a\":2},\"DATEKEY\":210,\"UUID\":\"user2\"}]";	
    	  SchemaPlus schemaPlus = Frameworks.createRootSchema(true);
          
  	    MultiJsonSchema jschema = new MultiJsonSchema(new ArrayList<String>(Arrays.asList("MYORDER", "MYPAGE")),
	    		new ArrayList<String>(Arrays.asList(json,jsona)));
  	    
  	  SchemaPlus splus= schemaPlus.add("MYSQL", jschema);
          //给schema T中添加表
     //     schemaPlus.add("T", new ReflectiveSchema(new TestSchema()));
    //	  SchemaPlus splus= schemaPlus.add("SCOTT", new JsonSchema("EMP", json));
    	  
          Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder();
          //设置默认schema
          configBuilder.defaultSchema(splus);

          FrameworkConfig frameworkConfig = configBuilder.build();

          SqlParser.ConfigBuilder paresrConfig = SqlParser.configBuilder(frameworkConfig.getParserConfig());
          
          
      //    paresrConfig.setConformance(SqlCo)
          //SQL 大小写不敏感
          paresrConfig.setCaseSensitive(false).setConfig(paresrConfig.build());

          Planner planner = Frameworks.getPlanner(frameworkConfig);

          SqlNode sqlNode;
          RelRoot relRoot = null;
          System.out.println("raw=");
          final String yourQuery = "SELECT t1.datekey, t1.book_succ_rate, t1.pay_order_cnt, t2.uv \n"+
        		  "FROM \n"+
        		  "(	// 产生指标book_succ_rate(预订成功率), pay_order_cnt(支付订单数)\n"+
        		  "SELECT t1.book_succ_cnt / t2.pay_order_cnt AS book_succ_rate, t2.pay_order_cnt AS pay_order_cnt FROM\n"+
        		  "( \n"+
        		  "SELECT datekey, count(1) AS book_succ_cnt FROM mysql.myorder WHERE is_book_success = 1 and datekey = 20190210 group by datekey\n"+
        		  ") t1 \n"+
        		  "LEFT JOIN\n"+
        		  "(\n"+
        		  "SELECT datekey, count(1) AS pay_order_cnt FROM mysql.myorder WHERE is_paid = 1 and datekey = 20190210 group by datekey\n"+
        		  ") t2\n"+
        		  "ON t1.datekey = t2.datekey\n"+
        		  ") t11\n"+
        		  "LEFT JOIN \n"+
        		  "(\n"+
        		  "SELECT datekey, count(distinct uuid) AS uv FROM mysql.mypage WHERE datekey = 20190210 group by datekey\n"+
        		  ") t2\n"+
        		  "ON t11.datekey = t2.datekey";
        		  
          try {
              //parser阶段
           //   sqlNode = planner.parse("select \"a\".\"s\", count(\"a\".\"s\") from \"T\".\"rdf\" \"a\" group by \"a\".\"s\"");
        	  sqlNode = planner.parse(yourQuery);
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
    
    @Test
    public void rolesqlTest()
    {
    	//\"IS_BOOK_SUCCESS\":220 \"is_PAID\":1
    
    	String json = "[{\"EMP_ID\":{\"a\":1},\"IS_BOOK_SUCCESS\":220,\"DATEKEY\":300,\"FIRST_NAME\":\"user1\",\"IS_PAID\":1},"
				+ "{\"DATEKEY\":310,\"EMP_ID\":{\"a\":2},\"IS_BOOK_SUCCESS\":210,\"FIRST_NAME\":\"user2\",\"IS_PAID\":1}]";
		
		String jsona = "[{\"EMP_ID\":{\"a\":1},\"ORGANIZATION\":220,\"DATEKEY\":300,\"UUID\":\"user1\"},"
				+ "{\"SALARY\":310,\"EMP_ID\":{\"a\":2},\"DATEKEY\":210,\"UUID\":\"user2\"}]";	
    	  SchemaPlus schemaPlus = Frameworks.createRootSchema(true);
          
  	    MultiJsonSchema jschema = new MultiJsonSchema(new ArrayList<String>(Arrays.asList("MYORDER", "MYPAGE")),
	    		new ArrayList<String>(Arrays.asList(json,jsona)));
  	    
  	  SchemaPlus splus= schemaPlus.add("MYSQL", jschema);
          //给schema T中添加表
     //     schemaPlus.add("T", new ReflectiveSchema(new TestSchema()));
    //	  SchemaPlus splus= schemaPlus.add("SCOTT", new JsonSchema("EMP", json));
    	  
          Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder();
          //设置默认schema
          configBuilder.defaultSchema(splus);

          FrameworkConfig frameworkConfig = configBuilder.build();

          SqlParser.ConfigBuilder paresrConfig = SqlParser.configBuilder(frameworkConfig.getParserConfig());
          
          //SQL 大小写不敏感
          paresrConfig.setCaseSensitive(false).setConfig(paresrConfig.setLex(Lex.MYSQL).build());

          Planner planner = Frameworks.getPlanner(frameworkConfig);

          SqlNode sqlNode;
          RelRoot relRoot = null;
          System.out.println("rolesql=");
          final String yourQuery = 
        		  "SELECT t1.datekey, t1.pay_order_cnt, t2.uv\n"+
        		  "FROM \n"+
        		  "(\n"+
        		  "SELECT datekey, count(1) AS pay_order_cnt FROM mysql.myorder WHERE is_paid = 1 and datekey = 20190210 group by datekey\n"+
        		  ") t1\n"+
        		  "LEFT JOIN\n"+ 
        		  "(\n"+
        		  "SELECT datekey, count(distinct uuid) AS uv FROM mysql.mypage WHERE datekey = 20190210 group by datekey\n"+
        		  ") t2\n"+
        		  "ON t1.datekey = t2.datekey\n";
        		 
          try {
              //parser阶段
           //   sqlNode = planner.parse("select \"a\".\"s\", count(\"a\".\"s\") from \"T\".\"rdf\" \"a\" group by \"a\".\"s\"");
        	  sqlNode = planner.parse(yourQuery);
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
