package com.bny.common.storm.components.spouts;

import backtype.storm.topology.IComponent;
import backtype.storm.topology.TopologyBuilder;

import com.bny.common.storm.components.IComponentConfig;

public interface ISpout<T extends IComponent> extends IComponentConfig {

    public String getComponentId();

    public void addToTopology(final TopologyBuilder builder);

}
