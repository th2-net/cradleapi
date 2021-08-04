/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

public class CassandraConnectionSettings
{
	private String host;
	private int port;
	private String username,
			password;
	private String localDataCenter;
	
	public CassandraConnectionSettings()
	{
		host = "";
		port = -1;
		username = "";
		password = "";
		localDataCenter = "";
	}

	public CassandraConnectionSettings(String host, int port, String localDataCenter)
	{
		this();
		this.host = host;
		this.port = port;
		this.localDataCenter = localDataCenter;
	}

	public CassandraConnectionSettings(CassandraConnectionSettings settings)
	{
		this.host = settings.host;
		this.port = settings.port;
		this.username = settings.username;
		this.password = settings.password;
		this.localDataCenter = settings.localDataCenter;
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
	
	
	public String getLocalDataCenter()
	{
		return localDataCenter;
	}
	
	public void setLocalDataCenter(String localDataCenter)
	{
		this.localDataCenter = localDataCenter;
	}
}
