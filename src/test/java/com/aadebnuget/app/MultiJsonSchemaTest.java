package com.aadebnuget.app;


import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
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
import com.aadebnuget.app.MultiJsonSchema;
public class MultiJsonSchemaTest {

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
	
@Test
public void shouldAnswerWithTrue()
{
	final FrameworkConfig config = config().build();
}

}
