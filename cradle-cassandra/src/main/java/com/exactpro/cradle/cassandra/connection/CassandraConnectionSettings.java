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

public class CassandraConnectionSettings
{
	private String localDataCenter;
	private String host;
	private int port;
	private String keyspace;
	private String username,
			password;

	public CassandraConnectionSettings()
	{
		localDataCenter = "";
		host = "";
		port = -1;
		keyspace = "";
		username = "";
		password = "";
	}

	public CassandraConnectionSettings(String localDataCenter, String host, int port, String keyspace)
	{
		super();
		this.localDataCenter = localDataCenter;
		this.host = host;
		this.port = port;
		this.keyspace = keyspace;
	}

	public CassandraConnectionSettings(CassandraConnectionSettings settings)
	{
		super();
		this.localDataCenter = settings.localDataCenter;
		this.host = settings.host;
		this.port = settings.port;
		this.keyspace = settings.keyspace;
		this.username = settings.username;
		this.password = settings.password;
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
}