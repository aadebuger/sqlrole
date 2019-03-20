package org.apache.calcite.plan.hep;



import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * HepRuleCall implements {@link RelOptRuleCall} for a {@link HepPlanner}. It
 * remembers transformation results so that the planner can choose which one (if
 * any) should replace the original expression.
 */
public class QuickRuleCall extends RelOptRuleCall {
  //~ Instance fields --------------------------------------------------------

  private List<RelNode> results;

  //~ Constructors -----------------------------------------------------------

  QuickRuleCall(
      RelOptPlanner planner,
      RelOptRuleOperand operand,
      RelNode[] rels,
      Map<RelNode, List<RelNode>> nodeChildren,
      List<RelNode> parents) {
    super(planner, operand, rels, nodeChildren, parents);

    results = new ArrayList<>();
  }

  //~ Methods ----------------------------------------------------------------

  // implement RelOptRuleCall
  public void transformTo(RelNode rel, Map<RelNode, RelNode> equiv) {
    final RelNode rel0 = rels[0];
    System.out.println("vo verify");
    //  RelOptUtil.verifyTypeEquivalence(rel0, rel, rel0);
    results.add(rel);
    rel(0).getCluster().invalidateMetadataQuery();
  }

  List<RelNode> getResults() {
    return results;
  }
}