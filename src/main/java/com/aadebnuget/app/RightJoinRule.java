package com.aadebnuget.app;


import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;

public class RightJoinRule extends RelOptRule {

public RightJoinRule(RelOptRuleOperand operand, String description) {
  super(operand, "Right_Join_rule:" + description);
}

public static final RightJoinRule INSTANCE =
    new RightJoinRule(
        operand(
            Join.class,none()
            ),
        "rightjoin_tableScan");

@Override
public void onMatch(RelOptRuleCall call) {
	System.out.println(" RightJoinRule onMatch ");
	   final Join join = call.rel(0);
	    final RelNode left = join.getLeft();
	    final HepRelVertex righthelp = (HepRelVertex)join.getRight();
	    final RelNode right = righthelp.getCurrentRel();
	    System.out.println("right="+right);
	    final RelBuilder relBuilder = call.builder();
	    
	    /*
	    final RelNode node = relBuilder
				  .scan("CKRQCVKVYO")	 
				  .build();
	    */
	    for (RexNode node: right.getChildExps())
	    {
	    	System.out.println("node"+node);
	    }
	    System.out.println("right.getChildExps() 1"+ right.getChildExps().get(0));
	    for (RelNode node: right.getInputs())
	    {
	    	System.out.println("node input "+node);
	    	
	    }
	    System.out.println("right.getInputs 0"+ right.getInputs().get(0));
	    final HepRelVertex inputvertex= (HepRelVertex)right.getInputs().get(0);
	    final RelNode input = inputvertex.getCurrentRel();
	    System.out.println("input node="+input);
	    if (right instanceof Project)
	    {
	    	System.out.println("right is project");
	    	Project newproject = (Project)right;
	    	//newproject.
	    }
	    System.out.println("query="+right.getQuery());
	    
	  //  System.out.println("getInput="+right.getInput(0));
	    System.out.println("getTable="+right.getTable());
	    System.out.println("rel="+ RelOptUtil.toString(right));
	  
	 //  call.transformTo(node);
	  //  call.transformTo(call.builder().push(join).empty().build());  // work ok
	    RelNode newnode =call.builder().push(right).empty().build();
	    System.out.println("newnode rowtype="+ newnode.getRowType());
	    RelNode newnode1 =call.builder().push(join).empty().build();
	    System.out.println("newnode1 rowtype="+ newnode1.getRowType());
	//    RelNode newnode2 =call.builder().push(right).empty().convert(newnode1.getRowType(), true).build();
	//    System.out.println("newnode2 rowtype="+ newnode2.getRowType()); 
	    
	    for (RelNode node: newnode.getInputs())
	    {
	    	System.out.println("node input"+node);
	    }
	    call.transformTo(call.builder().push(join).empty().push(input).build());
	 //   call.transformTo(call.builder().scan("HR.CKRQCVKVYO").build());
}
}