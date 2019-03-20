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

/**
 * Unit test for simple App.
 */
public class SqlT1Test 
{
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
	private static List<String> extractTableAliases(SqlNode node) {
        final List<String> tables = new ArrayList<>();

        // If order by comes in the query.
        if (node.getKind().equals(SqlKind.ORDER_BY)) {
            // Retrieve exact select.
            node = ((SqlSelect) ((SqlOrderBy) node).query).getFrom();
        } else {
            node = ((SqlSelect) node).getFrom();
        }

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
                info(from.getLeft());
                
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
	
	 private static Map<String, String> extractWhereClauses(SqlNode node) {
	        final Map<String, String> tableToPlaceHolder = new HashMap<>();

	        // If order by comes in the query.
	        if (node.getKind().equals(SqlKind.ORDER_BY)) {
	            // Retrieve exact select.
	            node = ((SqlOrderBy) node).query;
	        }

	        if (node == null) {
	            return tableToPlaceHolder;
	        }

	        final SqlBasicCall where = (SqlBasicCall) ((SqlSelect) node).getWhere();

	        if (where != null) {
	            // Case when there is only 1 where clause
	            if (where.operand(0).getKind().equals(SqlKind.IDENTIFIER)
	                    && where.operand(1).getKind().equals(SqlKind.LITERAL)) {
	                tableToPlaceHolder.put(where.operand(0).toString(), 
	                        where.operand(1).toString()); 
	                return tableToPlaceHolder;
	            }

	            final SqlBasicCall sqlBasicCallRight = where.operand(1);
	            SqlBasicCall sqlBasicCallLeft = where.operand(0);

	            // Iterate over left until we get a pair of identifier and literal.
	            while (!sqlBasicCallLeft.operand(0).getKind().equals(SqlKind.IDENTIFIER)
	                    && !sqlBasicCallLeft.operand(1).getKind().equals(SqlKind.LITERAL)) {
	                tableToPlaceHolder.put(((SqlBasicCall) sqlBasicCallLeft.operand(1)).operand(0).toString(), 
	                        ((SqlBasicCall) sqlBasicCallLeft.operand(1)).operand(1).toString()); 
	                sqlBasicCallLeft = sqlBasicCallLeft.operand(0); // Move to next where condition.
	            }

	            tableToPlaceHolder.put(sqlBasicCallLeft.operand(0).toString(), 
	                    sqlBasicCallLeft.operand(1).toString()); 
	            tableToPlaceHolder.put(sqlBasicCallRight.operand(0).toString(), 
	                    sqlBasicCallRight.operand(1).toString()); 
	            return tableToPlaceHolder;
	        }

	        return tableToPlaceHolder;
	    }
	 
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue()
    {
    	
    	try
    	{
    	
    	final String query = 
"SELECT t1.book_succ_cnt / t2.pay_order_cnt AS book_succ_rate, t2.pay_order_cnt AS pay_order_cnt FROM\n"+
"( \n"+
"SELECT datekey, count(1) AS book_succ_cnt FROM mysql.myorder WHERE is_book_success = 1 and datekey = 20190210\n"+
") t1 \n"+
"LEFT JOIN\n"+
"(\n"+
"SELECT datekey, count(1) AS pay_order_cnt FROM mysql.myorder WHERE is_paid = 1 and datekey = 20190210\n"+
") t2\n"+
"ON t1.datekey = t2.datekey\n";
    	final SqlParser parser = SqlParser.create(query);
            final SqlNode sqlNode = parser.parseQuery();
            final SqlSelect sqlSelect = (SqlSelect) sqlNode;
            final SqlJoin from = (SqlJoin) sqlSelect.getFrom();

            System.out.println("sqlSelect=");
            
            // Extract table names/data sets, For above SQL query : [e, s]
            final List<String> tables = extractTableAliases(sqlSelect);

				
            // Extract where clauses, For above SQL query : [e.organization -> 'Tesla', s.organization -> 'Tesla']
            final Map<String, String> whereClauses = extractWhereClauses(sqlSelect);
            
            System.out.println(tables);
            System.out.println(whereClauses);
    	}
    	catch( Exception e)
    	{
    		System.out.println(e);
    	}
    	}
    
    
   
}
