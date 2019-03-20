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
import com.aadebnuget.app.*;
public class SqlNodeUtilTest {

	  @Test
	    public void testselectFieldList()
	    {
			String yourQuery=
	    			"SELECT datekey,"+
	         "count(1) AS book_succ_cnt\n"+
	  "FROM mysql.myorder\n"+
	  "WHERE is_book_success = 1\n"+
	    "and datekey = 20190210";
		  List<String> fields =  new SqlNodeUtil().selectFieldList(yourQuery);
		  System.out.println("fields"+fields);
	    }
	  @Test
	    public void testselectFieldListright()
	    {
			String yourQuery=
	    			"SELECT datekey,\n"+
             "count(1) AS pay_order_cnt\n"+
      "FROM mysql.myorder\n"+
      "WHERE is_paid = 1\n"+
        "and datekey = 20190210 ";
		  List<String> fields =  new SqlNodeUtil().selectFieldList(yourQuery);
		  System.out.println("fields"+fields);
	    }
	  @Test
	    public void testselectFieldList1()
	    {
			String yourQuery=
	    			"SELECT t.datekey,\n"+
         " count(distinct uuid) AS uv\n"+
   "FROM mysql.page\n"+
   "WHERE datekey = 20190210";
		  List<String> fields =  new SqlNodeUtil().selectFieldList(yourQuery);
		  System.out.println("fields"+fields);
	    }
	  
	  
}
