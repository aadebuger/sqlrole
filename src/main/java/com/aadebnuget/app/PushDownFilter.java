package com.aadebnuget.app;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;

public class PushDownFilter extends RelOptRule {

public PushDownFilter(RelOptRuleOperand operand, String description) {
  super(operand, "Push_down_rule:" + description);
}

public static final PushDownFilter INSTANCE =
    new PushDownFilter(
        operand(
            Filter.class,
            operand(TableScan.class, none())),
        "filter_tableScan");

@Override
public void onMatch(RelOptRuleCall call) {
	System.out.println(" PushDownFilter onMatch ");
  LogicalFilter filter = (LogicalFilter) call.rels[0];
  TableScan tableScan = (TableScan) call.rels[1];
  // push down filter
  call.transformTo(tableScan);
}
}