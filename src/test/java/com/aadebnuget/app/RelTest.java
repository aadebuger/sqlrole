package com.aadebnuget.app;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import  com.aadebnuget.app.JsonSchema;

public class RelTest {
	  public static final JavaTypeFactory TYPE_FACTORY = new JavaTypeFactoryImpl(
		      RelDataTypeSystem.DEFAULT);
	  /*
	  public static RelDataType relDataType;
	public static void prepare() {
		  relDataType = TYPE_FACTORY.builder()
		      .add("order_id", SqlTypeName.BIGINT)
		      .add("site_id", SqlTypeName.INTEGER)
		      .add("price", SqlTypeName.DOUBLE)
		      .add("order_time", SqlTypeName.BIGINT).build();

		  beamRowType = CalciteUtils.toBeamRowType(relDataType);
		  record = new BeamRecord(beamRowType
		      , 1234567L, 0, 8.9, 1234567L);

		  SchemaPlus schema = Frameworks.createRootSchema(true);
		  final List<RelTraitDef> traitDefs = new ArrayList<>();
		  traitDefs.add(ConventionTraitDef.INSTANCE);
		  traitDefs.add(RelCollationTraitDef.INSTANCE);
		  FrameworkConfig config = Frameworks.newConfigBuilder()
		      .parserConfig(SqlParser.configBuilder().setLex(Lex.MYSQL).build()).defaultSchema(schema)
		      .traitDefs(traitDefs).context(Contexts.EMPTY_CONTEXT).ruleSets(BeamRuleSets.getRuleSets())
		      .costFactory(null).typeSystem(BeamRelDataTypeSystem.BEAM_REL_DATATYPE_SYSTEM).build();

		  relBuilder = RelBuilder.create(config);
		}
		 */
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
	  @Test
	    public void shouldAnswerWithTrue()
	    {
		  final FrameworkConfig config = config().build();
		  /*
		  FrameworkConfig calciteFrameworkConfig = Frameworks.newConfigBuilder()
			        .parserConfig(SqlParser.configBuilder()
			            // Lexical configuration defines how identifiers are quoted, whether they are converted to upper or lower
			            // case when they are read, and whether identifiers are matched case-sensitively.
			            .setLex(Lex.MYSQL)
			            .build())
			        // Sets the schema to use by the planner
			        .defaultSchema(schema)
			        .traitDefs(traitDefs)
			        // Context provides a way to store data within the planner session that can be accessed in planner rules.
			        .context(Contexts.EMPTY_CONTEXT)
			        // Rule sets to use in transformation phases. Each transformation phase can use a different set of rules.
			        .ruleSets(RuleSets.ofList())
			        // Custom cost factory to use during optimization
			        .costFactory(null)
			        .typeSystem(RelDataTypeSystem.DEFAULT)
			        .build();
		  */
		  final RelBuilder builder = RelBuilder.create(config);
		  
		  /*
		  
		  final RelNode node = builder.scan("EMP")
	        .aggregate(builder.groupKey("DEPTNO"),
	            builder.count(false, "C"),
	            builder.sum(false, "S", builder.field("SAL")))
	        .filter(
	            builder.call(SqlStdOperatorTable.GREATER_THAN, builder.field("C"),
	                builder.literal(10)));
		  */
		  final RelNode node = builder
				  .scan("EMP")
				  .build();
				System.out.println(RelOptUtil.toString(node));
				
		final RelNode node1 = builder
						  .scan("EMP")
						  .aggregate(builder.groupKey("CUST_ID"),
						      builder.count(false, "C"),
						      builder.sum(false, "S", builder.field("CUST_ID")))
						  .filter(
						      builder.call(SqlStdOperatorTable.GREATER_THAN,
						          builder.field("C"),
						          builder.literal(10)))
						  .build();
						System.out.println(RelOptUtil.toString(node1));
						
		final RelNode node2 =
							      builder.scan("EMP")
							          .project(builder.field("CUST_ID"))
							          .distinct()
							          .build();
		System.out.println(RelOptUtil.toString(node2));
	    }
	  @Test
	    public void twoTableTest()
	    {
		  final FrameworkConfig config = config().build();
	
		  final RelBuilder builder = RelBuilder.create(config);
		  
		
		  
				RelNode root =
					      builder.scan("DEPT")
					          .project(builder.field("CUST_ID"))
					          .scan("EMP")
					          .filter(
					              builder.call(SqlStdOperatorTable.EQUALS,
					                  builder.field("CUST_ID"),
					                  builder.literal(20)))
					          .project(builder.field("CUST_ID"))
					          .intersect(false)
					          .build();
				System.out.println("relnode="+RelOptUtil.toString(root));		
		  
	    }
	  @Test
	    public void projectTest()
	    {
		  final FrameworkConfig config = config().build();
	
		  final RelBuilder builder = RelBuilder.create(config);
		  
		
		  final RelNode node = builder
				  .scan("EMP")
				  .project(builder.field("CUST_ID"), builder.field("USER_ID"))
				  .build();
				System.out.println("prject="+RelOptUtil.toString(node));
			
		  
	    }
	  @Test
	    public void AggregateTest()
	    {
		  final FrameworkConfig config = config().build();
	
		  final RelBuilder builder = RelBuilder.create(config);
		  
		
		  final RelNode node = builder
				  .scan("EMP")
				  .aggregate(builder.groupKey("CUST_ID"),
				      builder.count(false, "C"),
				      builder.sum(false, "S", builder.field("PROD_ID")))
				  .build();
				System.out.println(RelOptUtil.toString(node));
			
		  
	    }
	  @Test
	    public void joinTest()
	    {
		  final FrameworkConfig config = config().build();
	
		  final RelBuilder builder = RelBuilder.create(config);
		  
		  /*
		  String jsona = "[{\"ORDER_ID\":{\"a\":1},\"PROD_ID\":220,\"USER_ID\":300,\"USER_NAME\":\"user1\"},"
					+ "{\"ORDER_ID\":310,\"DEPTNO\":{\"a\":2},\"PROD_ID\":210,\"USER_NAME\":\"user2\"}]";
		  final RelNode left = builder
				  .scan("CUSTOMERS")
				  .scan("ORDERS")
				  .join(JoinRelType.INNER, "ORDER_ID")
				  .build();

				final RelNode right = builder
				  .scan("LINE_ITEMS")
				  .scan("PRODUCTS")
				  .join(JoinRelType.INNER, "PRODUCT_ID")
				  .build();

				final RelNode result = builder
				  .push(left)
				  .push(right)
				  .join(JoinRelType.INNER, "ORDER_ID")
				  .build();
				System.out.println(RelOptUtil.toString(result));
			*/
		  //
		//  MysqlSqlDialect.DEFAULT;
		  final RelNode node = builder
				  .scan("EMP")
				  .aggregate(builder.groupKey("CUST_ID"),
				      builder.count(false, "C"),
				      builder.sum(false, "S", builder.field("PROD_ID")))
				  .build();
				System.out.println(RelOptUtil.toString(node));
		  RelToSqlConverter relToSqlConverter = new RelToSqlConverter(MysqlSqlDialect.DEFAULT);
		  RelToSqlConverter.Result res = relToSqlConverter.visitChild(0, node);
		  
		  SqlNode sqlNode = res.asQueryOrValues();
		  String result = sqlNode.toSqlString(MysqlSqlDialect.DEFAULT, false).getSql();
		  System.out.println("result="+result);
		  
		  
	    }
}
