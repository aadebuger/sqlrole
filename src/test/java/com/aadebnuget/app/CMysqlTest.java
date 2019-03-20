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
import org.apache.calcite.rex.RexNode;
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

public class CMysqlTest {

	 @Test
	    public void rolesqlTest()
	    {
	    	//\"IS_BOOK_SUCCESS\":220 \"is_PAID\":1     \"SUM_PAY_ROOMNIGH\":1 \"SUM_PAY_ROOMNIGHT\":2 \"COUNT_ORDER_ID\":3
	    
	    	String json = "[{\"EMP_ID\":{\"a\":1},\"IS_BOOK_SUCCESS\":220,\"SUM_PAY_AMOUNT\":300,\"PAY_TIME\":\"user1\",\"IS_PAID\":1,\"SUM_PAY_ROOMNIGH\":1,\"SUM_PAY_ROOMNIGHT\":2,\"COUNT_ORDER_ID\":3},"
					+ "{\"SUM_PAY_AMOUNT\":310,\"EMP_ID\":{\"a\":2},\"IS_BOOK_SUCCESS\":210,\"PAY_TIME\":\"user2\",\"IS_PAID\":1,\"SUM_PAY_ROOMNIGH\":1,\"SUM_PAY_ROOMNIGHT\":2,\"COUNT_ORDER_ID\":3}]";
			
			String jsona = "[{\"EMP_ID\":{\"a\":1},\"ORGANIZATION\":220,\"DATEKEY\":300,\"UUID\":\"user1\"},"
					+ "{\"SALARY\":310,\"EMP_ID\":{\"a\":2},\"DATEKEY\":210,\"UUID\":\"user2\"}]";	
	    	  SchemaPlus schemaPlus = Frameworks.createRootSchema(true);
	          
	  	    MultiJsonSchema jschema = new MultiJsonSchema(new ArrayList<String>(Arrays.asList("CKRQCVKVYO", "MYPAGE")),
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
	        		  "select t1.sum_pay_amount, \n"+
	   "t1.sum_pay_roomnight\n"+
"from \n"+
"(\n"+
	"select sum_pay_amount, sum_pay_roomnight, count_order_id \n"+
	"from CKRQCVKVYO \n"+
	"where is_paid = '1' and pay_time >= '2019-03-05 00:00:00' and pay_time <= '2019-03-05 23:59:59'\n"+
") t1";
	        		 
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
	    public void rawsqlTest()
	    {
	    	//\"IS_BOOK_SUCCESS\":220 \"is_PAID\":1     \"SUM_PAY_ROOMNIGH\":1 \"SUM_PAY_ROOMNIGHT\":2 \"COUNT_ORDER_ID\":3
	    // \"IS_ORDERED\":\"1\" \"ORDER_TIME\":\"1\" 
	    	String json = "[{\"EMP_ID\":{\"a\":1},\"IS_BOOK_SUCCESS\":220,\"SUM_PAY_AMOUNT\":300,\"PAY_TIME\":\"user1\",\"IS_PAID\":1,\"SUM_PAY_ROOMNIGH\":1,\"SUM_PAY_ROOMNIGHT\":2,\"COUNT_ORDER_ID\":3, \"IS_ORDERED\":\"1\", \"ORDER_TIME\":\"1\"  },"
					+ "{\"SUM_PAY_AMOUNT\":310,\"EMP_ID\":{\"a\":2},\"IS_BOOK_SUCCESS\":210,\"PAY_TIME\":\"user2\",\"IS_PAID\":1,\"SUM_PAY_ROOMNIGH\":1,\"SUM_PAY_ROOMNIGHT\":2,\"COUNT_ORDER_ID\":3, \"IS_ORDERED\":\"1\", \"ORDER_TIME\":\"1\"  }]";
			
			String jsona = "[{\"EMP_ID\":{\"a\":1},\"ORGANIZATION\":220,\"DATEKEY\":300,\"UUID\":\"user1\"},"
					+ "{\"SALARY\":310,\"EMP_ID\":{\"a\":2},\"DATEKEY\":210,\"UUID\":\"user2\"}]";	
	    	  SchemaPlus schemaPlus = Frameworks.createRootSchema(true);
	          
	  	    MultiJsonSchema jschema = new MultiJsonSchema(new ArrayList<String>(Arrays.asList("CKRQCVKVYO", "MYPAGE")),
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
	        		  "select t1.sum_pay_amount, 													-- 支付GMV\n"+
	   "t1.sum_pay_roomnight, 												-- 支付间夜\n"+
	   "t1.count_order_id AS payOrderCnt,									-- 支付订单量\n"+
	   "t5.count_order_id / t1.count_order_id AS orderCntVsPayOrderCnt		-- 下单订单量 - 支付订单量\n"+
"from \n"+
"(\n"+
	"select sum_pay_amount, sum_pay_roomnight, count_order_id \n"+
	"from CKRQCVKVYO \n"+
	"where is_paid = '1' and pay_time >= '2019-03-05 00:00:00' and pay_time <= '2019-03-05 23:59:59'\n"+
") t1,\n"+
"(\n"+
	"select count_order_id \n"+
	"from CKRQCVKVYO \n"+
	"where is_ordered = '1' and order_time >= '2019-03-05 00:00:00' and order_time <= '2019-03-05 23:59:59'\n"+
") t5";
	        		 
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
	          for ( RexNode item : relNode.getChildExps())
	          {
	        	  System.out.println(item);
	          }
	          
	    }
	 
}
