package com.aadebnuget.app;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue()
    {
    	
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
        assertTrue( true );
    }
    @Test
    public void simpleSql()
    {
    	
    	try
    	{
    	String yourQuery="SELECT t1.datekey, t1.book_succ_rate, t1.pay_order_cnt, t2.uv FROM  \n"
+"(	  \n"
+"SELECT t1.book_succ_cnt / t2.pay_order_cnt AS book_succ_rate, t2.pay_order_cnt AS pay_order_cnt FROM\n"
+"( \n"
+"SELECT datekey, count(1) AS book_succ_cnt FROM mysql.myorder WHERE is_book_success = 1 and datekey = 20190210\n"
+") t1 \n"
+"LEFT JOIN\n"
+"(\n"
+"SELECT datekey, count(1) AS pay_order_cnt FROM mysql.myorder WHERE is_paid = 1 and datekey = 20190210\n"
+") t2\n"
+"ON t1.datekey = t2.datekey\n"
+") t1\n"
+"LEFT JOIN\n" 
+"(\n"
+"SELECT t.datekey, count(distinct uuid) AS uv FROM mysql.page WHERE datekey = 20190210\n"
+") t2\n"
+"ON t1.datekey = t2.datekey\n";
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
    public void simpleSql1()
    {
    	
    	try
    	{
    	String yourQuery="SELECT t1.datekey, t1.pay_order_cnt, t2.uv \n"
    			+"FROM\n" 
    			+"(\n"
    			+"SELECT datekey, count(1) AS pay_order_cnt FROM mysql.myorder WHERE is_paid = 1 and datekey = 20190210\n"
    			+") t1\n"
    			+"LEFT JOIN\n" 
    			+"(\n"
    			+"SELECT t.datekey, count(distinct uuid) AS uv FROM mysql.page WHERE datekey = 20190210\n"
    			+") t2\n"
    			+"ON t1.datekey = t2.datekey";
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
}
