package com.aadebnuget.app;


import org.apache.calcite.schema.Schema;

import com.github.quxiucheng.tutorial.common.data.TutorialColumn;
import com.github.quxiucheng.tutorial.common.data.TutorialTable;
import com.github.quxiucheng.tutorial.common.data.TutorialTableSchema;

public class ApmData {

	/*
     static {
       
        TutorialColumn empid = new TutorialColumn("empid", "INTEGER", true);
        TutorialColumn deptno = new TutorialColumn("deptno", "INTEGER");
        TutorialColumn name = new TutorialColumn("name", "VARCHAR", 10);
        TutorialColumn salary = new TutorialColumn("salary", "FLOAT");
        TutorialColumn commission = new TutorialColumn("commission", "INTEGER");

        TutorialTable emps = new TutorialTable("emps", empid, deptno, name, salary, commission);

     

        TutorialColumn deptname = new TutorialColumn("name", "VARCHAR", 20);
        TutorialColumn createTime = new TutorialColumn("create_time", "DATE");
        TutorialTable depts = new TutorialTable("depts", deptno, deptname, createTime);

        TutorialTable depts2 = new TutorialTable("depts2", deptname, deptno, createTime);

//         hr = new TutorialTableSchema("hr", emps, depts, depts2);
    }
*/
    public static TutorialTableSchema hr()
    {
    	 TutorialColumn empid = new TutorialColumn("empid", "INTEGER", true);
         TutorialColumn deptno = new TutorialColumn("deptno", "INTEGER");
         TutorialColumn name = new TutorialColumn("name", "VARCHAR", 10);
         TutorialColumn salary = new TutorialColumn("salary", "FLOAT");
         TutorialColumn commission = new TutorialColumn("commission", "INTEGER");

         TutorialTable emps = new TutorialTable("emps", empid, deptno, name, salary, commission);

         /**
          public final int empid;
          public final String name;
          */

         TutorialColumn deptname = new TutorialColumn("name", "VARCHAR", 20);
         TutorialColumn createTime = new TutorialColumn("create_time", "DATE");
         TutorialTable depts = new TutorialTable("depts", deptno, deptname, createTime);

         TutorialTable depts2 = new TutorialTable("depts2", deptname, deptno, createTime);
    	return new TutorialTableSchema("hr", emps, depts, depts2);
    
    }
    public static TutorialTableSchema apmhr()
    {
    	 TutorialColumn empid = new TutorialColumn("empid", "INTEGER", true);
         TutorialColumn deptno = new TutorialColumn("IS_PAID", "INTEGER");
         TutorialColumn name = new TutorialColumn("name", "VARCHAR", 10);
         TutorialColumn salary = new TutorialColumn("salary", "FLOAT");
         TutorialColumn commission = new TutorialColumn("commission", "INTEGER");
         //pay_time
         TutorialColumn pay_time = new TutorialColumn("PAY_TIME", "VARCHAR", 20);
         TutorialColumn SUM_PAY_AMOUNT = new TutorialColumn("SUM_PAY_AMOUNT", "INTEGER");
         TutorialColumn SUM_PAY_ROOMNIGHT = new TutorialColumn("SUM_PAY_ROOMNIGHT", "INTEGER");
         TutorialColumn COUNT_ORDER_ID = new TutorialColumn("COUNT_ORDER_ID", "INTEGER");
         TutorialColumn IS_ORDERED = new TutorialColumn("IS_ORDERED", "INTEGER");
         TutorialColumn ORDER_TIME = new TutorialColumn("ORDER_TIME", "VARCHAR", 20);
         TutorialTable emps = new TutorialTable("CKRQCVKVYO", empid, deptno, name, salary, commission,pay_time,SUM_PAY_AMOUNT
        		 ,SUM_PAY_ROOMNIGHT
        		 ,COUNT_ORDER_ID
        		 ,IS_ORDERED
        		 ,ORDER_TIME);

         /**
          public final int empid;
          public final String name;
          */

         TutorialColumn deptname = new TutorialColumn("name", "VARCHAR", 20);
         TutorialColumn createTime = new TutorialColumn("create_time", "DATE");
         TutorialTable depts = new TutorialTable("depts", deptno, deptname, createTime);

         TutorialTable depts2 = new TutorialTable("depts2", deptname, deptno, createTime);
    	return new TutorialTableSchema("hr", emps, depts, depts2);
    
    }
}