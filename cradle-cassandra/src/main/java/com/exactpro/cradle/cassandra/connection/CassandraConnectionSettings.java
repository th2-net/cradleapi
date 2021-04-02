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

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.sun.tools.javac.util.StringUtils;

public class CassandraConnectionSettings
{
	public static final String DEFAULT_CERTIFICATE_TYPE = "X.509";
	public static final String DEFAULT_SSL_PROTOCOL = "TLS";
	
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
			resultPageSize;
	//Use to SSL connection
	private String certificatePath;
	private String certificateType = DEFAULT_CERTIFICATE_TYPE;
	private String certificatePassword;
	private String sslProtocol = DEFAULT_SSL_PROTOCOL;

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
		this.keyspace = StringUtils.toLowerCase(keyspace);
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

	public String getCertificatePath()
	{
		return certificatePath;
	}

	public void setCertificatePath(String certificatePath)
	{
		this.certificatePath = certificatePath;
	}

	public String getCertificateType()
	{
		return certificateType;
	}

	public void setCertificateType(String certificateType)
	{
		this.certificateType = certificateType;
	}

	public String getCertificatePassword()
	{
		return certificatePassword;
	}

	public void setCertificatePassword(String certificatePassword)
	{
		this.certificatePassword = certificatePassword;
	}

	public String getSslProtocol()
	{
		return sslProtocol;
	}

	public void setSslProtocol(String sslProtocol)
	{
		this.sslProtocol = sslProtocol;
	}
}
