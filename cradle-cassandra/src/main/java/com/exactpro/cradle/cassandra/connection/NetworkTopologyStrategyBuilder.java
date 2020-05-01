/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle.cassandra.connection;

import java.util.HashMap;
import java.util.Map;

public class NetworkTopologyStrategyBuilder
{
	private final Map<String, Integer> strategy = new HashMap<>();
	
	public NetworkTopologyStrategyBuilder add(String dataCenter, int replicationFactor)
	{
		strategy.put(dataCenter, replicationFactor);
		return this;
	}
	
	public NetworkTopologyStrategy build()
	{
		NetworkTopologyStrategy result = new NetworkTopologyStrategy(strategy);
		strategy.clear();
		return result;
	}
}
