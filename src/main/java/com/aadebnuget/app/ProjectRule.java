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

public class ProjectRule extends RelOptRule {

public ProjectRule(RelOptRuleOperand operand, String description) {
  super(operand, "Project_rule:" + description);
}

public static final ProjectRule INSTANCE =
    new ProjectRule(
        operand(
            Project.class,none()
            ),
        "project_tableScan");

@Override
public void onMatch(RelOptRuleCall call) {
	System.out.println(" Project onMatch ");
	   final Project project = call.rel(0);
	  // final Scan scan = call.rel(1);
	   System.out.println("rowtype="+project.getRowType());
	    System.out.println("project="+project);
	    final RelBuilder relBuilder = call.builder();
	    
	    /*
	    final RelNode node = relBuilder
				  .scan("CKRQCVKVYO")	 
				  .build();
	    */
	    for (RexNode node: project.getChildExps())
	    {
	    	System.out.println("node"+node);
	    }
	    RelNode stripped = project.getInput();
	    System.out.println("stripped="+stripped);
	    System.out.println("getNamedProjects"+project.getNamedProjects());
	    boolean bFind=false;
	    for ( Object item : project.getNamedProjects())
	    {
	    	Pair<RexNode, String> p=(Pair<RexNode, String>)item;
	    	System.out.println("item="+p.getKey());
	    	System.out.println("item value="+p.getValue());
	    	if (p.getValue().equals("PAYORDERCNT"))
	    		bFind=true;
	    }
	    System.out.println("bFind="+bFind);
	    List<RexNode> desiredFields = new ArrayList<>();
		List<String> desiredNames = new ArrayList<>();
		for (Pair<RexNode, String> field : project.getNamedProjects()) {
			
			/*
			if (field.getKey() instanceof RexInputRef) {
				desiredFields.add(field.getKey());
				desiredNames.add(field.getValue());
			}
			*/
			if (!field.getValue() .equals("PAYORDERCNT"))
			{
				desiredFields.add(field.getKey());
				desiredNames.add(field.getValue());
			}
		}
			

	    if (bFind)
	    {
	    	 
	    	RelNode newnode=call.builder().push(project.getInput()).project(desiredFields, desiredNames )       
            .build();
            
	    	RelNode child = call.getPlanner().register(newnode, project);
	        call.transformTo(child);
	    	
	    	// call.transformTo(
	    	          
	    	          
	    	              
	    	      
	    }
	 //  call.transformTo(node);
	 //   call.transformTo(call.builder().push(right).empty().build());
}
}