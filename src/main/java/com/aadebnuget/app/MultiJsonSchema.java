package com.aadebnuget.app;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link org.apache.calcite.schema.Schema} that exposes the
 * public fields and methods in a json.
 */
public class MultiJsonSchema extends AbstractSchema {
	private String target;
	private String topic;
	Map<String, Table> table = null;

	/**
	 * Creates a JsonSchema.
	 *
	 * @param target
	 *            Object whose fields will be sub-objects of the schema
	 */
	public MultiJsonSchema(List<String> topicv, List<String> targetv) {
		super();
		/*
		this.topic = topic;
		if (!target.startsWith("[")) {
			this.target = '[' + target + ']';
		} else {
			this.target = target;
		}*/
		
		final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
		for (int i = 0; i < topicv.size(); i++) {
			
		final Table table = fieldRelation(targetv.get(i));
		System.out.println("new table="+table);
		if (table != null) {
			builder.put(topicv.get(i), table);
		//	builder.put("DEPT", table);
			//this.table = builder.build();
			}
		}
		this.table = builder.build();
	}

	@Override
	public String toString() {
		return "JsonSchema(topic=" + topic + ":target=" + target + ")";
	}

	/**
	 * Returns the wrapped object.
	 *
	 * <p>
	 * May not appear to be used, but is used in generated code via
	 * {@link org.apache.calcite.util.BuiltInMethod#REFLECTIVE_SCHEMA_GET_TARGET}
	 * .
	 */
	public String getTarget() {
		return target;
	}

	@Override
	protected Map<String, Table> getTableMap() {
		return table;
	}

	/**
	 * Returns an expression for the object wrapped by this schema (not the
	 * schema itself).
	 */
	Expression getTargetExpression(SchemaPlus parentSchema, String name) {
		return Types.castIfNecessary(target.getClass(),
				Expressions.call(Schemas.unwrap(getExpression(parentSchema, name), JsonSchema.class),
						BuiltInMethod.REFLECTIVE_SCHEMA_GET_TARGET.method));
	}

	private <T> Table fieldRelation() {
		JSONArray jsonarr = JSON.parseArray(target);
		// final Enumerator<Object> enumerator = Linq4j.enumerator(list);
		return new JsonTable(jsonarr);
	}
	private <T> Table fieldRelation(String exttarget) {
		JSONArray jsonarr = JSON.parseArray(exttarget);
		// final Enumerator<Object> enumerator = Linq4j.enumerator(list);
		return new JsonTable(jsonarr);
	}

	private static class JsonTable extends AbstractTable implements ScannableTable {
		private final JSONArray jsonarr;
		// private final Enumerable<Object> enumerable;

		public JsonTable(JSONArray obj) {
			this.jsonarr = obj;
		}

		public RelDataType getRowType(RelDataTypeFactory typeFactory) {
			final List<RelDataType> types = new ArrayList<RelDataType>();
			final List<String> names = new ArrayList<String>();
			JSONObject jsonobj = jsonarr.getJSONObject(0);
			for (String string : jsonobj.keySet()) {
				final RelDataType type;

				type = typeFactory.createJavaType(jsonobj.get(string).getClass());

				names.add(string);
				types.add(type);
			}
			if (names.isEmpty()) {
				names.add("line");
				types.add(typeFactory.createJavaType(String.class));
			}
			return typeFactory.createStructType(Pair.zip(names, types));
		}

		public Statistic getStatistic() {
			return Statistics.UNKNOWN;
		}

		public Enumerable<Object[]> scan(DataContext root) {
			return new AbstractEnumerable<Object[]>() {
				@Override
				public Enumerator<Object[]> enumerator() {
					return new JsonEnumerator(jsonarr);
				}
			};
		}
	}

	public static class JsonEnumerator implements Enumerator<Object[]> {

		private Enumerator<Object[]> enumerator;

		public JsonEnumerator(JSONArray jsonarr) {
			List<Object[]> objs = new ArrayList<Object[]>();
			for (Object obj : jsonarr) {
				objs.add(((JSONObject) obj).values().toArray());
			}
			enumerator = Linq4j.enumerator(objs);
		}

		@Override
		public Object[] current() {
			return (Object[]) enumerator.current();
		}

		@Override
		public boolean moveNext() {
			return enumerator.moveNext();
		}

		@Override
		public void reset() {
			enumerator.reset();
		}

		@Override
		public void close() {
			enumerator.close();
		}

	}
}
