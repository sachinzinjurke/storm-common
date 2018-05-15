package com.shc.ecom.common.storm.components.bolt;

import backtype.storm.topology.IComponent;
import backtype.storm.topology.TopologyBuilder;

import com.shc.ecom.common.storm.components.IComponentConfig;


/**
 * [Class Description]
 *
 * @author Grant Henke
 * @since 12/5/12
 */
public interface IBolt<T extends IComponent> extends IComponentConfig {

    public String getComponentId();

    public T getStormBolt();

    public void addToTopology(final TopologyBuilder builder);

}
