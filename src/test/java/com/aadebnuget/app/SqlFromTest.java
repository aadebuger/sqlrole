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
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

public class SqlFromTest {

	
	private static void  info( SqlNode node)
		{
			System.out.println("classname=");
			if (node instanceof SqlSelect)
			{
				System.out.println("SqlSelect");
				SqlNode selectnode = ((SqlSelect) node).getFrom();
				info(selectnode);
				
			}
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
				
				
			}
			if (node instanceof SqlBasicCall)
			{
				System.out.println("SqlBasicCall");
			}
			if (node.getKind()==SqlKind.IDENTIFIER)
			{
				
				System.out.println("IDENTIFIER");
				
			}
			
			
			System.out.println("kind=");
			System.out.println(node.getKind());
			
		}
		
	 
	private static List<String> visit(SqlNode node) {
        final List<String> tables = new ArrayList<>();

        /*
        // If order by comes in the query.
        if (node.getKind().equals(SqlKind.ORDER_BY)) {
            // Retrieve exact select.
            node = ((SqlSelect) ((SqlOrderBy) node).query).getFrom();
        } else {
            node = ((SqlSelect) node).getFrom();
        }
		*/
    
    	
        if (node == null) {
            return tables;
        }

        // Case when only 1 data set in the query.
        if (node.getKind().equals(SqlKind.AS)) {
            tables.add(((SqlBasicCall) node).operand(1).toString());
            return tables;
        }

        // Case when there are more than 1 data sets in the query.
        if (node.getKind().equals(SqlKind.JOIN)) {
            final SqlJoin from = (SqlJoin) node;

            // Case when only 2 data sets are in the query.
            if (from.getLeft().getKind().equals(SqlKind.AS)) {
            	System.out.println("getLeft kind as");
                tables.add(((SqlBasicCall) from.getLeft()).operand(1).toString());
                System.out.println("from.getLeft()");
                System.out.println(from.getLeft());
                
            } else {
                // Case when more than 2 data sets are in the query.
                SqlJoin left = (SqlJoin) from.getLeft();

                // Traverse until we get a AS.
                while (!left.getLeft().getKind().equals(SqlKind.AS)) {
                    tables.add(((SqlBasicCall) left.getRight()).operand(1).toString());
                    left = (SqlJoin) left.getLeft();
                }
                System.out.println("table left right");
                tables.add(((SqlBasicCall) left.getLeft()).operand(1).toString());
                tables.add(((SqlBasicCall) left.getRight()).operand(1).toString());
            }
            System.out.println("table from");
            System.out.println("from.getRight()");
            System.out.println(from.getRight());
            tables.add(((SqlBasicCall) from.getRight()).operand(1).toString());
            return tables;
        }

        return tables;
    }
	private static void  infovisit( SqlNode node)
	{
		
		if (node instanceof SqlSelect)
		{
			//System.out.println("SqlSelect");
			SqlNode fromnode = ((SqlSelect) node).getFrom();
			if (fromnode.getKind()==SqlKind.IDENTIFIER)
			{
				System.out.println(node.toString());
			}
			else
			{
				SqlNodeList list = ((SqlSelect)node).getSelectList();

		    	for (int i = 0; i < list.size(); i++) {
		    	    System.out.println("Column " + (i + 1) + ": " + list.get(i).toString());
		    	}
				infovisit(fromnode);
			
			}
		}
		if (node  instanceof SqlJoin)
		{
			System.out.println("SqlJoin Left");
			final SqlJoin from = (SqlJoin) node;
			infovisit(from.getLeft());
			System.out.println("SqlJoin Right");
			infovisit((from.getRight()));
		}
		if (node instanceof SqlBasicCall)
		{
			// System.out.println("SqlBasicCall");
			if (node.getKind().equals(SqlKind.AS))
			{
			//	System.out.println("SqlBasicCall AS");
			//	System.out.println("call="+((SqlBasicCall) node).operand(0).toString());
				infovisit(((SqlBasicCall) node).operand(0));
			}
			else
			{
			for (SqlNode operand : ((SqlBasicCall) node).getOperands()) {
				System.out.println("operand");
			     System.out.println(operand);
			      }
			//System.out.println("operands="+((SqlBasicCall) node).operand(1).toString());
			}
		
		}
	
		if (node.getKind()==SqlKind.IDENTIFIER)
		{
			
		
			System.out.println("IDENTIFIER:"+node.toString());
		}
	
		
		System.out.println("kind="+node.getKind());
		
	}
	
	
    @Test
    public void testFrom()
    {
    	
    	try
    	{
    	
    	final String query = "SELECT t1.datekey, t1.book_succ_rate, t1.pay_order_cnt, t2.uv \n"+
"FROM \n"+
"(	// 产生指标book_succ_rate(预订成功率), pay_order_cnt(支付订单数)\n"+
"SELECT t1.book_succ_cnt / t2.pay_order_cnt AS book_succ_rate, t2.pay_order_cnt AS pay_order_cnt FROM\n"+
"( \n"+
"SELECT datekey, count(1) AS book_succ_cnt FROM mysql.myorder WHERE is_book_success = 1 and datekey = 20190210\n"+
") t1 \n"+
"LEFT JOIN\n"+
"(\n"+
"SELECT datekey, count(1) AS pay_order_cnt FROM mysql.myorder WHERE is_paid = 1 and datekey = 20190210\n"+
") t2\n"+
"ON t1.datekey = t2.datekey\n"+
") t1\n"+
"LEFT JOIN \n"+
"(\n"+
"SELECT t.datekey, count(distinct uuid) AS uv FROM mysql.mypage WHERE datekey = 20190210\n"+
") t2\n"+
"ON t1.datekey = t2.datekey";
    	final SqlParser parser = SqlParser.create(query);
            final SqlNode sqlNode = parser.parseQuery();
            final SqlSelect sqlSelect = (SqlSelect) sqlNode;
            final SqlJoin from = (SqlJoin) sqlSelect.getFrom();

            System.out.println("sqlFrom=");
            
            System.out.println( from.toString());
           // visit(from);
            infovisit(from);
    	}
    	catch( Exception e)
    	{
    		System.out.println(e);
    	}
    	}
    
}
