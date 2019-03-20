package com.aadebnuget.app;



import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParser;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;

import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
public class SqlNodeUtil {

	
	 void  info(List<String> fields ,SqlNode node)
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
			System.out.println("operands="+((SqlBasicCall) node).operand(1).toString());
			
			fields.add(((SqlBasicCall) node).operand(1).toString());
		}
		if (node instanceof SqlBasicCall)
		{
			System.out.println("SqlBasicCall");
		}
		if (node.getKind()==SqlKind.IDENTIFIER)
		{
			fields.add(node.toString());
			System.out.println("IDENTIFIER");
			
		}
		
		
		System.out.println("kind=");
		System.out.println(node.getKind());
		
	}
	
	public List<String>   selectFieldList(String yourQuery)
    {
		 final List<String> fields = new ArrayList<>();
    	try
    	{
    
    			
 //   	yourQuery="SELECT t1.datekey, t1.book_succ_rate, t1.pay_order_cnt, t2.uv FROM  mysql.myorder";
    	System.out.println(yourQuery);
    	SqlParser parser = SqlParser.create(yourQuery);
    	SqlSelect selectNode = (SqlSelect) parser.parseQuery();
    	SqlNodeList list = selectNode.getSelectList();

    	for (int i = 0; i < list.size(); i++) {
    	    System.out.println("Column " + (i + 1) + ": " + list.get(i).toString());
    	    info(fields,list.get(i));
    	}
    	}
    	catch(Exception e)
    	{
    		System.out.println(e);
    		System.out.println("exception");
    	}
      return fields;
    }
}
