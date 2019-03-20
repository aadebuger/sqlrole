package com.aadebnuget.app;

import static org.junit.Assert.assertTrue;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.Test;

public class SelectTest {

	
	static void  info(SqlNode node)
	{
		System.out.println("classname=");
		if (node  instanceof SqlJoin)
		{
			System.out.println("SqlJoin");
		}
		if (node instanceof SqlBasicCall)
		{
			System.out.println("SqlBasicCall");
			for (SqlNode operand : ((SqlBasicCall) node).getOperands()) {
				System.out.println("operand");
			     System.out.println(operand);
			      }
			      
		}
		if (node instanceof SqlBasicCall)
		{
			System.out.println("SqlBasicCall");
		}
		
		System.out.println("kind=");
		System.out.println(node.getKind());
		
	}
	  @Test
	    public void simpleSql2()
	    {
	    	
	    	try
	    	{
	    	String yourQuery=
	    			"SELECT datekey FROM myorder WHERE is_paid = 1 and datekey = 20190210\n";
	    			
	 //   	yourQuery="SELECT t1.datekey, t1.book_succ_rate, t1.pay_order_cnt, t2.uv FROM  mysql.myorder";
	    	System.out.println(yourQuery);
	    	SqlParser parser = SqlParser.create(yourQuery);
	    	SqlSelect selectNode = (SqlSelect) parser.parseQuery();
	    	SqlNodeList list = selectNode.getSelectList();

	    	for (int i = 0; i < list.size(); i++) {
	    	    System.out.println("Column " + (i + 1) + ": " + list.get(i).toString());
	    	}
	    	}
	    	catch(Exception e)
	    	{
	    		System.out.println(e);
	    		System.out.println("exception");
	    	}
	        assertTrue( true );
	    }
	  @Test
	    public void simpleSql3()
	    {
	    	
	    	try
	    	{
	    	String yourQuery=
	    			"SELECT datekey,"+
             "count(1) AS book_succ_cnt\n"+
      "FROM mysql.myorder\n"+
      "WHERE is_book_success = 1\n"+
        "and datekey = 20190210";
	    			
	 //   	yourQuery="SELECT t1.datekey, t1.book_succ_rate, t1.pay_order_cnt, t2.uv FROM  mysql.myorder";
	    	System.out.println(yourQuery);
	    	SqlParser parser = SqlParser.create(yourQuery);
	    	SqlSelect selectNode = (SqlSelect) parser.parseQuery();
	    	SqlNodeList list = selectNode.getSelectList();

	    	for (int i = 0; i < list.size(); i++) {
	    	    System.out.println("Column " + (i + 1) + ": " + list.get(i).toString());
	    	    info(list.get(i));
	    	}
	    	}
	    	catch(Exception e)
	    	{
	    		System.out.println(e);
	    		System.out.println("exception");
	    	}
	        assertTrue( true );
	    }
}
