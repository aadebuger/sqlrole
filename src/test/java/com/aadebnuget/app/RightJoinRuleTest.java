package com.aadebnuget.app;



import org.junit.Test;

import com.github.quxiucheng.tutorial.common.catalog.TutorialCalciteCatalogReader;
import com.github.quxiucheng.tutorial.common.parser.SqlParserBase;
import com.github.quxiucheng.tutorial.common.sql2rel.SqlToRelConverterBase;
import com.github.quxiucheng.tutorial.common.sql2rel.TutorialSqlToRelConverter;
import com.github.quxiucheng.tutorial.common.validate.TutorialSqlValidator;
//import com.github.quxiucheng.tutorial.rule.RuleTester;
import com.google.common.collect.Lists;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.hep.QuickPlanner;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.PlannerImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.rules.ProjectSetOpTransposeRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;

import com.aadebnuget.app.MultiJsonSchema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RightJoinRuleTest {

    public static void apmrelprintProcessRule(RelRoot relRoot, RelOptRule... rules) {

        try {
        	
            System.out.println("test12");
            // 原始
            RelNode original = relRoot.rel;
            System.out.println("原始:");
            System.out.println(RelOptUtil.toString(original));
            System.out.println();
            RelNode temp = original;
            System.out.println("test2");
  		  RelToSqlConverter relToSqlConverter = new RelToSqlConverter(MysqlSqlDialect.DEFAULT);

		  
            for (RelOptRule rule : rules) {
                HepProgramBuilder builder = HepProgram.builder();
                builder.addRuleInstance(rule);
                HepProgram program = builder.build();
                HepPlanner hepPlanner = new HepPlanner(program);
                hepPlanner.setRoot(temp);
                temp = hepPlanner.findBestExp();
                System.out.println("规则:" + rule.toString());
                System.out.println(RelOptUtil.toString(temp));
                System.out.println();
                RelToSqlConverter.Result res = relToSqlConverter.visitChild(0, temp);
			  
                SqlNode newsqlNode = res.asQueryOrValues();
			  
      		  	String result = newsqlNode.toSqlString(MysqlSqlDialect.DEFAULT, false).getSql();
      		  	System.out.println("sql result="+result);
    		  
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
    public static void vapmrelprintProcessRule(RelRoot relRoot, RelOptRule... rules) {

        try {
        	
            System.out.println("test12");
            // 原始
            RelNode original = relRoot.rel;
            System.out.println("原始:");
            System.out.println(RelOptUtil.toString(original));
            System.out.println();
            RelNode temp = original;
            System.out.println("test2");
  		  RelToSqlConverter relToSqlConverter = new RelToSqlConverter(MysqlSqlDialect.DEFAULT);

		  
            for (RelOptRule rule : rules) {
                HepProgramBuilder builder = HepProgram.builder();
                builder.addRuleInstance(rule);
                HepProgram program = builder.build();
       //.        VolcanoPlanner hepPlanner = new VolcanoPlanner();
                QuickPlanner hepPlanner = new QuickPlanner(program);
                
                hepPlanner.setRoot(temp);
                temp = hepPlanner.findBestExp();
                System.out.println("规则:" + rule.toString());
                System.out.println(RelOptUtil.toString(temp));
                System.out.println();
          
			  
                
               
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        
        /*
        try {
            RelToSqlConverter.Result res = relToSqlConverter.visitChild(0, temp);
			  
            SqlNode newsqlNode = res.asQueryOrValues();
		  	String result = newsqlNode.toSqlString(MysqlSqlDialect.DEFAULT, false).getSql();
		  	System.out.println("sql result="+result);
        }
        catch (Exception e) {
           System.out.println(e);
           
        }
	  
        */
    }
	   @Test
	    public void apmRuleMathTest()
	    {

//	          final     String sql = "select e.name as ename,d.name as dname from hr.emps e join hr.depts d on e.deptno = d.deptno where e.name = '1'";
	    
	     	  SchemaPlus schemaPlus = Frameworks.createRootSchema(true);
	          
	    
	    	    
	    	  SchemaPlus splus= schemaPlus.add("HR", ApmData.apmhr());
	  	    System.out.println("splus gettablenames="+splus.getTableNames());
	  	    
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
	  
	      
	      
	    	  
	          final String sql = 
	        		  "select t1.sum_pay_amount, 													-- 支付GMV\n"+
	   "t1.sum_pay_roomnight, 												-- 支付间夜\n"+
	   "t1.count_order_id AS payOrderCnt,									-- 支付订单量\n"+
	   "t5.count_order_id / t1.count_order_id AS orderCntVsPayOrderCnt		-- 下单订单量 - 支付订单量\n"+
	"from \n"+
	"(\n"+
	"select sum_pay_amount, sum_pay_roomnight, count_order_id \n"+
	"from hr.CKRQCVKVYO \n"+
	"where is_paid = '1' and pay_time >= '2019-03-05 00:00:00' and pay_time <= '2019-03-05 23:59:59'\n"+
	") t1,\n"+
	"(\n"+
	"select count_order_id \n"+
	"from hr.CKRQCVKVYO \n"+
	"where is_ordered = '1' and order_time >= '2019-03-05 00:00:00' and order_time <= '2019-03-05 23:59:59'\n"+
	") t5";
	          try {
	              //parser阶段
	           //   sqlNode = planner.parse("select \"a\".\"s\", count(\"a\".\"s\") from \"T\".\"rdf\" \"a\" group by \"a\".\"s\"");
	        	  sqlNode = planner.parse(sql);
	        	  //validate阶段
	              planner.validate(sqlNode);
	              //获取RelNode树的根
	              relRoot = planner.rel(sqlNode);
	   
	              System.out.print(RelOptUtil.toString(relRoot.project()));
	              vapmrelprintProcessRule(relRoot,
	            	//	  TwoProjectRule.INSTANCE);
	                     RightJoinRule.INSTANCE);
	              System.out.println("test quick end ");

	          } catch (Exception e) {
	              e.printStackTrace();
	          }
	          
	        
	          
	    }
}