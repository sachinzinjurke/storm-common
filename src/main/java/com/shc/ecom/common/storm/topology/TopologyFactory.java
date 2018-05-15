package com.shc.ecom.common.storm.topology;

import java.util.List;

import org.springframework.beans.factory.FactoryBean;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;

import com.shc.ecom.common.storm.components.bolt.GenericBolt;
import com.shc.ecom.common.storm.components.spouts.GenericSpout;

@SuppressWarnings("rawtypes")
public class TopologyFactory implements FactoryBean {

	private final List<IRichSpout> spouts;
	private final List<IRichBolt> bolts;

	public TopologyFactory(final List<IRichSpout> spout, final List<IRichBolt> bolt) {
		this.spouts = spout;
		this.bolts = bolt;
	}

	public StormTopology getObject() throws Exception {
		final TopologyBuilder builder = new TopologyBuilder();

		setTopologySpouts(builder);
		setTopologyBolts(builder);

		return builder.createTopology();
	}

	private void setTopologySpouts(final TopologyBuilder builder) {
		for (IRichSpout spout : spouts) {
			// spout.addToTopology(builder);
			builder.setSpout( ((GenericSpout) spout).getComponentId(), 
					spout, ((GenericSpout) spout).getParallelismHint());
		}
	}

	private void setTopologyBolts(final TopologyBuilder builder) {
		for (IRichBolt bolt : bolts) {
			// bolt.addToTopology(builder);
			BoltDeclarer boltDeclarer = builder.setBolt(((GenericBolt) bolt).getComponentId(), 
					bolt, ((GenericBolt) bolt).getParallelismHint());
			//((GenericBolt) bolt).getParallelismHint()).setNumTasks(200);
			((GenericBolt) bolt).addBoltGroupingsToBolt(boltDeclarer);
		}
	}

	public Class<?> getObjectType() {
		return StormTopology.class;
	}

	public boolean isSingleton() {
		return false;
	}
}
