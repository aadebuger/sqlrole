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
public class RuleTest {

	/*
	public static SqlToRelConverter jsoncreateSqlToRelConverterlite(SqlParser.Config parserConfig,
            SqlToRelConverter.Config sqlToRelConverterConfig) {
CalciteCatalogReader catalogReader = JsonCalciteCatalogReader.createMockCatalogReader(parserConfig);
VolcanoPlanner planner = new VolcanoPlanner(RelOptCostImpl.FACTORY, Contexts.of(catalogReader.getConfig()));
planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
return jsoncreateSqlToRelConverter(parserConfig, sqlToRelConverterConfig, planner);
}
*/
	
	public static SqlToRelConverter apmcreateSqlToRelConverterlite(SqlParser.Config parserConfig,
            SqlToRelConverter.Config sqlToRelConverterConfig) {
CalciteCatalogReader catalogReader = ApmCalciteCatalogReader.createMockCatalogReader(parserConfig);
VolcanoPlanner planner = new VolcanoPlanner(RelOptCostImpl.FACTORY, Contexts.of(catalogReader.getConfig()));
planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
System.out.println("test101");
return apmcreateSqlToRelConverter(parserConfig, sqlToRelConverterConfig, planner);
}
	

	public static SqlToRelConverter createSqlToRelConverterlite(SqlParser.Config parserConfig,
            SqlToRelConverter.Config sqlToRelConverterConfig) {
CalciteCatalogReader catalogReader = TutorialCalciteCatalogReader.createMockCatalogReader(parserConfig);
VolcanoPlanner planner = new VolcanoPlanner(RelOptCostImpl.FACTORY, Contexts.of(catalogReader.getConfig()));
planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
return createSqlToRelConverter(parserConfig, sqlToRelConverterConfig, planner);
}
	
    public static SqlToRelConverter createSqlToRelConverter(SqlParser.Config parserConfig,
                                                            SqlToRelConverter.Config sqlToRelConverterConfig,
                                                            RelOptPlanner planner) {

        PlannerImpl plannerImpl = new PlannerImpl(Frameworks
                .newConfigBuilder()
                //不知道是啥
                // .traitDefs(ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE)
                .sqlToRelConverterConfig(sqlToRelConverterConfig)
                .parserConfig(parserConfig)
                .build());
        RexBuilder rexBuilder = new RexBuilder(new JavaTypeFactoryImpl());
        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
        CalciteCatalogReader catalogReader = TutorialCalciteCatalogReader.createMockCatalogReader(parserConfig);
        SqlValidatorImpl validator = TutorialSqlValidator.createMockSqlValidator(parserConfig);

        return new SqlToRelConverter(
                plannerImpl,
                validator,
                catalogReader,
                cluster,
                StandardConvertletTable.INSTANCE,
                sqlToRelConverterConfig);
    }
    /*
    public static SqlToRelConverter jsoncreateSqlToRelConverter(SqlParser.Config parserConfig,
            SqlToRelConverter.Config sqlToRelConverterConfig,
            RelOptPlanner planner) {

PlannerImpl plannerImpl = new PlannerImpl(Frameworks
.newConfigBuilder()
//不知道是啥
// .traitDefs(ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE)
.sqlToRelConverterConfig(sqlToRelConverterConfig)
.parserConfig(parserConfig)
.build());
RexBuilder rexBuilder = new RexBuilder(new JavaTypeFactoryImpl());
RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
CalciteCatalogReader catalogReader = JsonCalciteCatalogReader.createMockCatalogReader(parserConfig);
SqlValidatorImpl validator = TutorialSqlValidator.createMockSqlValidator(parserConfig);

return new SqlToRelConverter(
plannerImpl,
validator,
catalogReader,
cluster,
StandardConvertletTable.INSTANCE,
sqlToRelConverterConfig);
}
*/
    public static SqlToRelConverter apmcreateSqlToRelConverter(SqlParser.Config parserConfig,
            SqlToRelConverter.Config sqlToRelConverterConfig,
            RelOptPlanner planner) {

PlannerImpl plannerImpl = new PlannerImpl(Frameworks
.newConfigBuilder()
//不知道是啥
// .traitDefs(ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE)
.sqlToRelConverterConfig(sqlToRelConverterConfig)
.parserConfig(parserConfig)
.build());
RexBuilder rexBuilder = new RexBuilder(new JavaTypeFactoryImpl());
RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
CalciteCatalogReader catalogReader = ApmCalciteCatalogReader.createMockCatalogReader(parserConfig);
SqlValidatorImpl validator = TutorialSqlValidator.createMockSqlValidator(parserConfig);

return new SqlToRelConverter(
plannerImpl,
validator,
catalogReader,
cluster,
StandardConvertletTable.INSTANCE,
sqlToRelConverterConfig);
}
    
    
	 public static void printOriginalCompare(String sql, RelOptRule... rules) {
	        HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();
	        printOriginalCompare(sql, hepProgramBuilder, rules);
	    }

	    public static void printOriginalCompare(String sql, HepProgramBuilder builder, RelOptRule... rules) {
	        try {
	            SqlParser.Config mysql = SqlParser.configBuilder().setLex(Lex.MYSQL).setCaseSensitive(false).build();
	            SqlToRelConverter.Config aDefault = SqlToRelConverterBase.DEFAULT;

	            SqlParserBase sqlParserBase = new SqlParserBase();
	            SqlNode sqlNode = sqlParserBase.parseQuery(sql);
	            SqlToRelConverter sqlToRelConverter = TutorialSqlToRelConverter.createSqlToRelConverter(mysql, aDefault);
	            RelRoot relRoot = sqlToRelConverter.convertQuery(sqlNode, true, true);
	            RelNode original = relRoot.rel;
	            for (RelOptRule rule : rules) {
	                builder.addRuleInstance(rule);
	            }
	            HepProgram program = builder.build();
	            HepPlanner hepPlanner = new HepPlanner(program);
	            List<RelMetadataProvider> plist = Lists.newArrayList();
	            plist.add(DefaultRelMetadataProvider.INSTANCE);
//            hepPlanner.registerMetadataProviders(Lists.newArrayList(DefaultRelMetadataProvider.INSTANCE));
	            hepPlanner.registerMetadataProviders(plist);
	        	
	            
	            hepPlanner.setRoot(original);
	            // hepPlanner优化
	            RelNode bestExp = hepPlanner.findBestExp();
	            System.out.println("sql:");
	            System.out.println(sql);
	            System.out.println();
	            System.out.println("原始:");
	            System.out.println(RelOptUtil.toString(original));
	            System.out.println();
	            System.out.println("优化后:");
	            System.out.println(RelOptUtil.toString(bestExp));
	        } catch (Exception e) {
	            throw new RuntimeException(e);
	        }

	    }
	    /*
	    public static void jsonprintOriginalCompare(String sql, HepProgramBuilder builder, RelOptRule... rules) {
	        try {
	            SqlParser.Config mysql = SqlParser.configBuilder().setLex(Lex.MYSQL).setCaseSensitive(false).build();
	            SqlToRelConverter.Config aDefault = SqlToRelConverterBase.DEFAULT;

	            SqlParserBase sqlParserBase = new SqlParserBase();
	            SqlNode sqlNode = sqlParserBase.parseQuery(sql);
	            SqlToRelConverter sqlToRelConverter = jsoncreateSqlToRelConverterlite(mysql, aDefault);
	            RelRoot relRoot = sqlToRelConverter.convertQuery(sqlNode, true, true);
	            RelNode original = relRoot.rel;
	            for (RelOptRule rule : rules) {
	                builder.addRuleInstance(rule);
	            }
	            HepProgram program = builder.build();
	            HepPlanner hepPlanner = new HepPlanner(program);
	            List<RelMetadataProvider> plist = Lists.newArrayList();
	            plist.add(DefaultRelMetadataProvider.INSTANCE);
//            hepPlanner.registerMetadataProviders(Lists.newArrayList(DefaultRelMetadataProvider.INSTANCE));
	            hepPlanner.registerMetadataProviders(plist);
	        	
	            
	            hepPlanner.setRoot(original);
	            // hepPlanner优化
	            RelNode bestExp = hepPlanner.findBestExp();
	            System.out.println("sql:");
	            System.out.println(sql);
	            System.out.println();
	            System.out.println("原始:");
	            System.out.println(RelOptUtil.toString(original));
	            System.out.println();
	            System.out.println("优化后:");
	            System.out.println(RelOptUtil.toString(bestExp));
	        } catch (Exception e) {
	        	
	            throw new RuntimeException(e);
	        }

	    }
	    */
	    /*
	    public static void jsonprintProcessRule(String sql, RelOptRule... rules) {

	        try {
	            System.out.println("sql:");
	            System.out.println(sql);
	            System.out.println();
	            SqlParser.Config mysql = SqlParser.configBuilder().setLex(Lex.MYSQL).setCaseSensitive(false).build();
	            SqlToRelConverter.Config aDefault = SqlToRelConverterBase.DEFAULT;

	            SqlParserBase sqlParserBase = new SqlParserBase();
	            SqlNode sqlNode = sqlParserBase.parseQuery(sql);
	            SqlToRelConverter sqlToRelConverter = jsoncreateSqlToRelConverterlite(mysql, aDefault);
	            RelRoot relRoot = sqlToRelConverter.convertQuery(sqlNode, true, true);
	            // 原始
	            RelNode original = relRoot.rel;
	            System.out.println("原始:");
	            System.out.println(RelOptUtil.toString(original));
	            System.out.println();
	            RelNode temp = original;
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
	        	e.printStackTrace();
	            throw new RuntimeException(e);
	        }

	    }
	    */
	    public static void apmprintProcessRule(String sql, RelOptRule... rules) {

	        try {
	            System.out.println("sql:");
	            System.out.println(sql);
	            System.out.println();
	            SqlParser.Config mysql = SqlParser.configBuilder().setLex(Lex.MYSQL).setCaseSensitive(false).build();
	            SqlToRelConverter.Config aDefault = SqlToRelConverterBase.DEFAULT;
	            System.out.println("test1");
	            
	            SqlParserBase sqlParserBase = new SqlParserBase();
	            SqlNode sqlNode = sqlParserBase.parseQuery(sql);
	            System.out.println("test10");
	            SqlToRelConverter sqlToRelConverter = apmcreateSqlToRelConverterlite(mysql, aDefault);
	            System.out.println("test11");
	            RelRoot relRoot = sqlToRelConverter.convertQuery(sqlNode, true, true);
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
	    public static void apmrelprintProcessRule(RelRoot relRoot, RelOptRule... rules) {

	        try {
	        	/*
	            System.out.println("sql:");
	            System.out.println(sql);
	            System.out.println();
	            SqlParser.Config mysql = SqlParser.configBuilder().setLex(Lex.MYSQL).setCaseSensitive(false).build();
	            SqlToRelConverter.Config aDefault = SqlToRelConverterBase.DEFAULT;
	            System.out.println("test1");
	            
	            SqlParserBase sqlParserBase = new SqlParserBase();
	            SqlNode sqlNode = sqlParserBase.parseQuery(sql);
	            System.out.println("test10");
	            SqlToRelConverter sqlToRelConverter = apmcreateSqlToRelConverterlite(mysql, aDefault);
	            System.out.println("test11");
	            RelRoot relRoot = sqlToRelConverter.convertQuery(sqlNode, true, true);
	            */
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
	    public static void printProcessRule(String sql, RelOptRule... rules) {

	        try {
	            System.out.println("sql:");
	            System.out.println(sql);
	            System.out.println();
	            SqlParser.Config mysql = SqlParser.configBuilder().setLex(Lex.MYSQL).setCaseSensitive(false).build();
	            SqlToRelConverter.Config aDefault = SqlToRelConverterBase.DEFAULT;

	            SqlParserBase sqlParserBase = new SqlParserBase();
	            SqlNode sqlNode = sqlParserBase.parseQuery(sql);
	      //      SqlToRelConverter sqlToRelConverter = TutorialSqlToRelConverter.createSqlToRelConverter(mysql, aDefault);
	        // test local
	            SqlToRelConverter sqlToRelConverter = createSqlToRelConverterlite(mysql, aDefault);
		        
	            RelRoot relRoot = sqlToRelConverter.convertQuery(sqlNode, true, true);
	            // 原始
	            RelNode original = relRoot.rel;
	            System.out.println("原始:");
	            System.out.println(RelOptUtil.toString(original));
	            System.out.println();
	            RelNode temp = original;
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
	    
    @Test
    public void empsTest()
    {
    	System.out.println("emps");
    	//\"IS_BOOK_SUCCESS\":220 \"is_PAID\":1
    
    	String json = "[{\"EMP_ID\":{\"a\":1},\"IS_BOOK_SUCCESS\":220,\"DATEKEY\":300,\"SALARY\":\"user1\",\"IS_PAID\":1},"
				+ "{\"DATEKEY\":310,\"EMP_ID\":{\"a\":2},\"IS_BOOK_SUCCESS\":210,\"SALARY\":\"user2\",\"IS_PAID\":1}]";
		
		String jsona = "[{\"EMP_ID\":{\"a\":1},\"ORGANIZATION\":220,\"DATEKEY\":300,\"UUID\":\"user1\"},"
				+ "{\"SALARY\":310,\"EMP_ID\":{\"a\":2},\"DATEKEY\":210,\"UUID\":\"user2\"}]";	
    	  SchemaPlus schemaPlus = Frameworks.createRootSchema(true);
          
  	    MultiJsonSchema jschema = new MultiJsonSchema(new ArrayList<String>(Arrays.asList("EMPS", "MYPAGE")),
	    		new ArrayList<String>(Arrays.asList(json,jsona)));
  	    
  	  SchemaPlus splus= schemaPlus.add("HR", jschema);
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
          final 
        	        String sql = "select salary from " +
        	                "(select * from hr.emps e1 " +
        	                "union all " +
        	                "select * from hr.emps e2) ";
          try {
              //parser阶段
           //   sqlNode = planner.parse("select \"a\".\"s\", count(\"a\".\"s\") from \"T\".\"rdf\" \"a\" group by \"a\".\"s\"");
        	  sqlNode = planner.parse(sql);
        	  //validate阶段
              planner.validate(sqlNode);
              //获取RelNode树的根
              relRoot = planner.rel(sqlNode);
          } catch (Exception e) {
              e.printStackTrace();
          }

          RelNode relNode = relRoot.project();
          System.out.print(RelOptUtil.toString(relNode));
          printProcessRule(sql,
                  // 将project(投影) 下推到 SetOp(例如:union ,minus, except)
                  ProjectSetOpTransposeRule.INSTANCE,
                  ProjectRemoveRule.INSTANCE);
          
    }
    //    //FilterJoinRule
    @Test
    public void FilterJoinRuleTest()
    {
    	System.out.println("FilterJoinRuleTest");
    	//\"IS_BOOK_SUCCESS\":220 \"is_PAID\":1
    	//\"NAME\":\"1\"
    	String json = "[{\"DEPTNO\":{\"a\":1},\"IS_BOOK_SUCCESS\":220,\"DATEKEY\":300,\"SALARY\":\"user1\",\"IS_PAID\":1,\"NAME\":\"1\"},"
				+ "{\"DATEKEY\":310,\"DEPTNO\":{\"a\":2},\"IS_BOOK_SUCCESS\":210,\"SALARY\":\"user2\",\"IS_PAID\":1,\"NAME\":\"1\"}]";
		
		String jsona = "[{\"DEPTNO\":{\"a\":1},\"ORGANIZATION\":220,\"DATEKEY\":300,\"UUID\":\"user1\",\"NAME\":\"1\"},"
				+ "{\"SALARY\":310,\"DEPTNO\":{\"a\":2},\"DATEKEY\":210,\"UUID\":\"user2\",\"NAME\":\"1\"}]";	
    	  SchemaPlus schemaPlus = Frameworks.createRootSchema(true);
          
  	    MultiJsonSchema jschema = new MultiJsonSchema(new ArrayList<String>(Arrays.asList("EMPS", "DEPTS")),
	    		new ArrayList<String>(Arrays.asList(json,jsona)));
  	    
  	  SchemaPlus splus= schemaPlus.add("HR", jschema);
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
          final 
          String sql = "select e.name as ename,d.name as dname from hr.emps e join hr.depts d on e.deptno = d.deptno where e.name = '1'";
          try {
              //parser阶段
           //   sqlNode = planner.parse("select \"a\".\"s\", count(\"a\".\"s\") from \"T\".\"rdf\" \"a\" group by \"a\".\"s\"");
        	  sqlNode = planner.parse(sql);
        	  //validate阶段
              planner.validate(sqlNode);
              //获取RelNode树的根
              relRoot = planner.rel(sqlNode);
          } catch (Exception e) {
              e.printStackTrace();
          }

          RelNode relNode = relRoot.project();
          System.out.print(RelOptUtil.toString(relNode));
          printProcessRule(sql,
                  // 将project(投影) 下推到 SetOp(例如:union ,minus, except)
                  ProjectSetOpTransposeRule.INSTANCE,
                  ProjectRemoveRule.INSTANCE);
          
    }
    
    @Test
    public void jsonFilterJoinRuleTest()
    {
    	System.out.println("jsonFilterJoinRuleTest");
    	//\"IS_BOOK_SUCCESS\":220 \"is_PAID\":1
    	//\"NAME\":\"1\"
    	String json = "[{\"DEPTNO\":{\"a\":1},\"IS_BOOK_SUCCESS\":220,\"DATEKEY\":300,\"SALARY\":\"user1\",\"IS_PAID\":1,\"NAME\":\"1\"},"
				+ "{\"DATEKEY\":310,\"DEPTNO\":{\"a\":2},\"IS_BOOK_SUCCESS\":210,\"SALARY\":\"user2\",\"IS_PAID\":1,\"NAME\":\"1\"}]";
		
		String jsona = "[{\"DEPTNO\":{\"a\":1},\"ORGANIZATION\":220,\"DATEKEY\":300,\"UUID\":\"user1\",\"NAME\":\"1\"},"
				+ "{\"SALARY\":310,\"DEPTNO\":{\"a\":2},\"DATEKEY\":210,\"UUID\":\"user2\",\"NAME\":\"1\"}]";	
    	  SchemaPlus schemaPlus = Frameworks.createRootSchema(true);
          
  	    MultiJsonSchema jschema = new MultiJsonSchema(new ArrayList<String>(Arrays.asList("EMPS", "DEPTS")),
	    		new ArrayList<String>(Arrays.asList(json,jsona)));
  	    
  	  SchemaPlus splus= schemaPlus.add("HR", jschema);
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
          final 
          String sql = "select e.name as ename,d.name as dname from hr.emps e join hr.depts d on e.deptno = d.deptno where e.name = '1'";
          try {
              //parser阶段
           //   sqlNode = planner.parse("select \"a\".\"s\", count(\"a\".\"s\") from \"T\".\"rdf\" \"a\" group by \"a\".\"s\"");
        	  sqlNode = planner.parse(sql);
        	  //validate阶段
              planner.validate(sqlNode);
              //获取RelNode树的根
              relRoot = planner.rel(sqlNode);
          } catch (Exception e) {
              e.printStackTrace();
          }

          RelNode relNode = relRoot.project();
          System.out.print(RelOptUtil.toString(relNode));
          /*
          jsonprintProcessRule(sql,
                  // 将project(投影) 下推到 SetOp(例如:union ,minus, except)
                  ProjectSetOpTransposeRule.INSTANCE,
                  ProjectRemoveRule.INSTANCE);
          */
    }
    
    @Test
    public void apmjsonFilterJoinRuleTest()
    {

//          final     String sql = "select e.name as ename,d.name as dname from hr.emps e join hr.depts d on e.deptno = d.deptno where e.name = '1'";
    
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
              apmrelprintProcessRule(relRoot,
                      // 将project(投影) 下推到 SetOp(例如:union ,minus, except)
                      ProjectSetOpTransposeRule.INSTANCE,
                      ProjectRemoveRule.INSTANCE);
              System.out.println("test quick end ");

          } catch (Exception e) {
              e.printStackTrace();
          }
          
          System.out.println("test begin");

          
          apmprintProcessRule(sql,
                  // 将project(投影) 下推到 SetOp(例如:union ,minus, except)
                  ProjectSetOpTransposeRule.INSTANCE,
                  ProjectRemoveRule.INSTANCE);
          System.out.println("test end");
          
    }
    
}
