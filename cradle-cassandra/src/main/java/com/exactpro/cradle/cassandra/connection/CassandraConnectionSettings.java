/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.cradle.cassandra.connection;

import java.util.concurrent.ExecutorService;

import com.datastax.oss.driver.api.core.ConsistencyLevel;

public class CassandraConnectionSettings
{
	private String localDataCenter;
	private String host;
	private int port;
	private String keyspace;
	private String username,
			password;
	private long timeout;
	private ConsistencyLevel writeConsistencyLevel,
			readConsistencyLevel;
	private NetworkTopologyStrategy networkTopologyStrategy;
	private int maxParallelQueries,
			resultPageSize,
			maxSyncRetries,
			maxAsyncRetries,
			minRetryDelay,
			maxRetryDelay;
	private ExecutorService composingService;

	public CassandraConnectionSettings()
	{
		localDataCenter = "";
		host = "";
		port = -1;
		keyspace = "";
		username = "";
		password = "";
		timeout = 0;
		writeConsistencyLevel = null;
		readConsistencyLevel = null;
		networkTopologyStrategy = null;
		maxParallelQueries = 500;
		resultPageSize = 0;  //In this case default page size will be used
		maxSyncRetries = 3;
		maxAsyncRetries = -1;  //Infinite "retry till success"
		minRetryDelay = 100;
		maxRetryDelay = 60000;
		composingService = null;
	}

	public CassandraConnectionSettings(String localDataCenter, String host, int port, String keyspace)
	{
		this();
		this.localDataCenter = localDataCenter;
		this.host = host;
		this.port = port;
		this.keyspace = keyspace;
	}

	public CassandraConnectionSettings(CassandraConnectionSettings settings)
	{
		this();
		this.localDataCenter = settings.localDataCenter;
		this.host = settings.host;
		this.port = settings.port;
		this.keyspace = settings.keyspace;
		this.username = settings.username;
		this.password = settings.password;
		this.timeout = settings.timeout;
		this.writeConsistencyLevel = settings.writeConsistencyLevel;
		this.readConsistencyLevel = settings.readConsistencyLevel;
		this.networkTopologyStrategy = settings.getNetworkTopologyStrategy() != null ? new NetworkTopologyStrategy(settings.getNetworkTopologyStrategy().asMap()) : null;
		this.maxParallelQueries = settings.maxParallelQueries;
		this.resultPageSize = settings.resultPageSize;
		this.maxSyncRetries = settings.maxSyncRetries;
		this.maxAsyncRetries = settings.maxAsyncRetries;
		this.minRetryDelay = settings.minRetryDelay;
		this.maxRetryDelay = settings.maxRetryDelay;
		this.composingService = settings.composingService;
	}

	
	public String getLocalDataCenter()
	{
		return localDataCenter;
	}
	
	public void setLocalDataCenter(String localDataCenter)
	{
		this.localDataCenter = localDataCenter;
	}
	
	
	public String getHost()
	{
		return host;
	}
	
	public void setHost(String host)
	{
		this.host = host;
	}
	
	
	public int getPort()
	{
		return port;
	}
	
	public void setPort(int port)
	{
		this.port = port;
	}
	
	
	public String getKeyspace()
	{
		return keyspace;
	}
	
	public void setKeyspace(String keyspace)
	{
		this.keyspace = keyspace;
	}
	
	
	public String getUsername()
	{
		return username;
	}
	
	public void setUsername(String username)
	{
		this.username = username;
	}
	
	
	public String getPassword()
	{
		return password;
	}
	
	public void setPassword(String password)
	{
		this.password = password;
	}
	
	
	public long getTimeout()
	{
		return timeout;
	}
	
	public void setTimeout(long timeout)
	{
		this.timeout = timeout;
	}
	
	
	public ConsistencyLevel getWriteConsistencyLevel()
	{
		return writeConsistencyLevel;
	}
	
	public void setWriteConsistencyLevel(ConsistencyLevel writeConsistencyLevel)
	{
		this.writeConsistencyLevel = writeConsistencyLevel;
	}
	
	
	public ConsistencyLevel getReadConsistencyLevel()
	{
		return readConsistencyLevel;
	}
	
	public void setReadConsistencyLevel(ConsistencyLevel readConsistencyLevel)
	{
		this.readConsistencyLevel = readConsistencyLevel;
	}
	
	
	public NetworkTopologyStrategy getNetworkTopologyStrategy()
	{
		return networkTopologyStrategy;
	}
	
	public void setNetworkTopologyStrategy(NetworkTopologyStrategy networkTopologyStrategy)
	{
		this.networkTopologyStrategy = networkTopologyStrategy;
	}
	
	
	public int getMaxParallelQueries()
	{
		return maxParallelQueries;
	}
	
	public void setMaxParallelQueries(int maxParallelQueries)
	{
		this.maxParallelQueries = maxParallelQueries;
	}
	
	
	public int getResultPageSize()
	{
		return resultPageSize;
	}
	
	public void setResultPageSize(int resultPageSize)
	{
		this.resultPageSize = resultPageSize;
	}
	
	
	public int getMaxSyncRetries()
	{
		return maxSyncRetries;
	}
	
	public void setMaxSyncRetries(int maxSyncRetries)
	{
		this.maxSyncRetries = maxSyncRetries;
	}
	
	
	public int getMaxAsyncRetries()
	{
		return maxAsyncRetries;
	}
	
	public void setMaxAsyncRetries(int maxAsyncRetries)
	{
		this.maxAsyncRetries = maxAsyncRetries;
	}
	
	
	public int getMinRetryDelay()
	{
		return minRetryDelay;
	}
	
	public void setMinRetryDelay(int minRetryDelay)
	{
		this.minRetryDelay = minRetryDelay;
	}
	
	public int getMaxRetryDelay()
	{
		return maxRetryDelay;
	}
	
	public void setMaxRetryDelay(int maxRetryDelay)
	{
		this.maxRetryDelay = maxRetryDelay;
	}
	
	
	public ExecutorService getComposingService()
	{
		return composingService;
	}
	
	public void setComposingService(ExecutorService composingService)
	{
		this.composingService = composingService;
	}
}
