package com.aadebnuget.app;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.FilterCorrelateRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Programs;
import com.aadebnuget.app.MultiJsonSchema;
public class HelpPlannerTest {

	@Test
	public void shouldAnswerWithTrue()
	{
		 HepProgram program = HepProgram.builder()
			//	 .addRuleInstance(new AdjustProjectForCountAggregateRule(false))
			//	 .addRuleInstance(new AdjustProjectForCountAggregateRule(true))
				 .addRuleInstance(FilterJoinRule.FILTER_ON_JOIN)
				 .addRuleInstance(FilterProjectTransposeRule.INSTANCE).addRuleInstance(FilterCorrelateRule.INSTANCE).build();
		 HepPlanner planner = createPlanner(program);
		    planner.setRoot(root);
		    root = planner.findBestExp();
		    // Perform decorrelation.
		    map.clear();
		    final Frame frame = getInvoke(root, null);
		    if (frame != null) {
		        // has been rewritten; apply rules post-decorrelation
		        final HepProgram program2 = HepProgram.builder().addRuleInstance(FilterJoinRule.FILTER_ON_JOIN).addRuleInstance(FilterJoinRule.JOIN).build();
		        final HepPlanner planner2 = createPlanner(program2);
		        final RelNode newRoot = frame.r;
		        planner2.setRoot(newRoot);
		        return planner2.findBestExp();
		    }
	}
	
}
