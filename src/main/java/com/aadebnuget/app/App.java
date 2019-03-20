package com.aadebnuget.app;



import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
/**
 * Hello world!
 *
 */
/*
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
    }
}
*/

public class App {
	/*
    public static class TestSchema {
        public final Triple[] rdf = {new Triple("s", "p", "o")};
    }
    */

    public static void main(String[] args) {
    	
    	/*
        SchemaPlus schemaPlus = Frameworks.createRootSchema(true);
        
        //给schema T中添加表
        schemaPlus.add("T", new ReflectiveSchema(new TestSchema()));
        Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder();
        //设置默认schema
        configBuilder.defaultSchema(schemaPlus);

        FrameworkConfig frameworkConfig = configBuilder.build();

        SqlParser.ConfigBuilder paresrConfig = SqlParser.configBuilder(frameworkConfig.getParserConfig());
        
        //SQL 大小写不敏感
        paresrConfig.setCaseSensitive(false).setConfig(paresrConfig.build());

        Planner planner = Frameworks.getPlanner(frameworkConfig);

        SqlNode sqlNode;
        RelRoot relRoot = null;
        try {
            //parser阶段
            sqlNode = planner.parse("select \"a\".\"s\", count(\"a\".\"s\") from \"T\".\"rdf\" \"a\" group by \"a\".\"s\"");
            //validate阶段
            planner.validate(sqlNode);
            //获取RelNode树的根
            relRoot = planner.rel(sqlNode);
        } catch (Exception e) {
            e.printStackTrace();
        }

        RelNode relNode = relRoot.project();
        System.out.print(RelOptUtil.toString(relNode));
        */
    	try
    	{
    	String yourQuery="select * from myorder";
    	SqlParser parser = SqlParser.create(yourQuery);
    	SqlSelect selectNode = (SqlSelect) parser.parseQuery();
    	SqlNodeList list = selectNode.getSelectList();

    	for (int i = 0; i < list.size(); i++) {
    	    System.out.println("Column " + (i + 1) + ": " + list.get(i).toString());
    	}
    	}
    	catch(Exception e)
    	{
    		System.out.println("exception");
    	}
    }
}
