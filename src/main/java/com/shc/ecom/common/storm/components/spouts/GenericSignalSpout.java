/**
 * 
 */
package com.shc.ecom.common.storm.components.spouts;

import java.io.Serializable;
import java.util.Locale;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.support.ReloadableResourceBundleMessageSource;

import backtype.storm.topology.TopologyBuilder;


/**
 * @author djohn0
 *
 */
public abstract class GenericSignalSpout extends GenericSpout implements Watcher, Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1926029416771383423L;

	private static final Logger LOG = LoggerFactory.getLogger(GenericSignalSpout.class.getName());
	
	@Autowired 
	@Qualifier("myProperties")
	protected ReloadableResourceBundleMessageSource myProperties;
	
    private static final String namespace = "storm-signals";
    private String name;
    private CuratorFramework client;

    public GenericSignalSpout(String componentId, String name) {
    	super(componentId);
        this.name = name;
    }
    
    protected void init() {
    	try {
        	initZookeeper();
        } catch (Exception e) {
            LOG.error("Error creating zookeeper client.", e);
        }
    }

    private void initZookeeper() throws Exception {
        String connectString = myProperties.getMessage("storm.zookeeper.servers", null, "localhost:2181", Locale.US);
        int retryCount = Integer.parseInt(myProperties.getMessage("storm.zookeeper.retry.times", null, "5", Locale.US));
        int retryInterval = Integer.parseInt(myProperties.getMessage("storm.zookeeper.retry.interval", null, "1000", Locale.US));

        this.client = CuratorFrameworkFactory.builder().namespace(namespace).connectString(connectString)
                .retryPolicy(new RetryNTimes(retryCount, retryInterval)).build();
        this.client.start();

        // create base path if necessary
        Stat stat = this.client.checkExists().usingWatcher(this).forPath(this.name);
        if (stat == null) {
            String path = this.client.create().creatingParentsIfNeeded().forPath(this.name);
            LOG.info("Created: " + path);
        }
    }

    /**
     * Releases the zookeeper connection.
     */
    protected void close() {
        this.client.close();
    }

    @Override
    public void process(WatchedEvent we) {
        try {
            this.client.checkExists().usingWatcher(this).forPath(this.name);
            LOG.debug("Renewed watch for path %s", this.name);
        } catch (Exception ex) {
            LOG.error("Error renewing watch.", ex);
        }

        switch (we.getType()) {
        case NodeCreated:
            LOG.debug("Node created.");
            break;
        case NodeDataChanged:
            LOG.debug("Received signal.");
            try {
                this.onSignal(this.client.getData().forPath(we.getPath()));
            } catch (Exception e) {
                LOG.warn("Unable to process signal.", e);
            }
            break;
        case NodeDeleted:
            LOG.debug("NodeDeleted");
            break;
		default:
			break;
        }

    }

    protected abstract void onSignal(byte[] data);

	@Override
	public void addToTopology(TopologyBuilder builder) {
		// TODO Auto-generated method stub
		
	}



}
