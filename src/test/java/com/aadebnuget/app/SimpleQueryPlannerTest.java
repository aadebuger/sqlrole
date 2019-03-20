package com.aadebnuget.app;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.junit.Test;

public class SimpleQueryPlannerTest {

	
	static Planner queryPlanner;
	
	private static  RelNode getLogicalPlan(String query) throws ValidationException, RelConversionException {
	    SqlNode sqlNode;

	    try {
	      sqlNode = queryPlanner.parse(query);
	    } catch (SqlParseException e) {
	      throw new RuntimeException("Query parsing error.", e);
	    }

	    SqlNode validatedSqlNode = queryPlanner.validate(sqlNode);

	    return queryPlanner.rel(validatedSqlNode).project();
	  }
	
	private static void newSimpleQuwryPlanner(SchemaPlus schema)
	{
		 final List<RelTraitDef> traitDefs = new ArrayList<RelTraitDef>();

		    traitDefs.add(ConventionTraitDef.INSTANCE);
		    traitDefs.add(RelCollationTraitDef.INSTANCE);

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
		//        .ruleSets(RuleSets.ofList())
		        // Custom cost factory to use during optimization
		        .costFactory(null)
		        .typeSystem(RelDataTypeSystem.DEFAULT)
		        .build();
		    queryPlanner = Frameworks.getPlanner(calciteFrameworkConfig);
	}
	@Test
	public void testSimpleplanner()
	{
		
		String json = "[{\"CUST_ID\":{\"a\":1},\"product\":220,\"USER_ID\":300,\"USER_NAME\":\"user1\"},"
				+ "{\"USER_ID\":310,\"CUST_ID\":{\"a\":2},\"product\":210,\"USER_NAME\":\"user2\"}]";
		String jsona = "[{\"DEPTNO\":{\"a\":1},\"PROD_ID\":220,\"USER_ID\":300,\"USER_NAME\":\"user1\"},"
				+ "{\"USER_ID\":310,\"DEPTNO\":{\"a\":2},\"PROD_ID\":210,\"USER_NAME\":\"user2\"}]";
	    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
	    
	    MultiJsonSchema jschema = new MultiJsonSchema(new ArrayList<String>(Arrays.asList("orders", "DEPT")),
	    		new ArrayList<String>(Arrays.asList(json,jsona)));
	    // bad 
	    //    SchemaPlus splus= rootSchema.add("SCOTT", new JsonSchema("EMP", json)).add("SCOTT", new JsonSchema("DEPT", jsona));
	//    SchemaPlus splus= rootSchema.add("SCOTT", new JsonSchema("EMP", json));
	    SchemaPlus splus= rootSchema.add("SCOTT", jschema);
		  
	    newSimpleQuwryPlanner(splus);
	//	SimpleQueryPlanner queryPlanner = new SimpleQueryPlanner(connection.getRootSchema().getSubSchema(connection.getSchema()));
		
	    try 
	    {
		RelNode loginalPlan = getLogicalPlan("select product from orders");
	    System.out.println(RelOptUtil.toString(loginalPlan));
	    }
	    catch( Exception e )
	    {
	    	System.out.println(e);
	    }
	}
}
