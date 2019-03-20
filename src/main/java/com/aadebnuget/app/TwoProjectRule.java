package com.aadebnuget.app;


import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;

public class TwoProjectRule extends RelOptRule {

public TwoProjectRule(RelOptRuleOperand operand, String description) {
  super(operand, "Project_rule:" + description);
}

public static final TwoProjectRule INSTANCE =
    new TwoProjectRule(
        operand(
            Project.class,operand(Project.class,none())
            ),
        "project_tableScan");

@Override
public void onMatch(RelOptRuleCall call) {
	System.out.println(" two Project onMatch ");
	   final Project project0 = call.rel(0);
	      call.transformTo(project0);
	    	
	    	// call.transformTo(
	    	          
	    	          
	  
}
}