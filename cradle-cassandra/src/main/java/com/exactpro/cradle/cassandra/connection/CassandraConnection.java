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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;

import java.net.InetSocketAddress;
import java.util.Date;

import org.apache.commons.lang3.StringUtils;

public class CassandraConnection
{
	private CassandraConnectionSettings settings;
	private CqlSession session;
	private Date started,
			stopped;

	public CassandraConnection()
	{
		super();
		this.settings = createSettings();
	}

	public CassandraConnection(CassandraConnectionSettings settings)
	{
		super();
		this.settings = settings;
	}

	
	public void start() throws Exception
	{
		CqlSessionBuilder sessionBuilder = CqlSession.builder();
		if (!StringUtils.isEmpty(settings.getLocalDataCenter()))
			sessionBuilder = sessionBuilder.withLocalDatacenter(settings.getLocalDataCenter());
		if (settings.getPort() > -1)
			sessionBuilder = sessionBuilder.addContactPoint(new InetSocketAddress(settings.getHost(), settings.getPort()));
		if (!StringUtils.isEmpty(settings.getUsername()))
			sessionBuilder = sessionBuilder.withAuthCredentials(settings.getUsername(), settings.getPassword());
		session = sessionBuilder.build();
		started = new Date();
	}

	public void stop() throws Exception
	{
		if (session != null)
			session.close();
		stopped = new Date();
	}
	
	
	public CassandraConnectionSettings getSettings()
	{
		return settings;
	}
	
	public void setSettings(CassandraConnectionSettings settings)
	{
		this.settings = settings;
	}
	
	
	public CqlSession getSession()
	{
		return session;
	}
	
	public Date getStarted()
	{
		return started;
	}
	
	public Date getStopped()
	{
		return stopped;
	}
	
	
	protected CassandraConnectionSettings createSettings()
	{
		return new CassandraConnectionSettings();
	}
}
