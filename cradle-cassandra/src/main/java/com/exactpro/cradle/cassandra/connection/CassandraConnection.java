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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import org.apache.commons.lang3.StringUtils;

import static com.exactpro.cradle.cassandra.CassandraStorageSettings.DEFAULT_TIMEOUT;

public class CassandraConnection
{
	private final CassandraConnectionSettings settings;
	private volatile CqlSession session;
	private volatile Instant started,
			stopped;
	private final long timeout;

	public CassandraConnection()
	{
		this(new CassandraConnectionSettings(), DEFAULT_TIMEOUT);
	}

	public CassandraConnection(CassandraConnectionSettings settings, long timeout)
	{
		this.settings = settings;
		this.timeout = timeout;
	}

	
	public void start() throws Exception
	{
		CqlSessionBuilder sessionBuilder = CqlSession.builder();
		sessionBuilder.withConfigLoader(getConfigLoader());
		if (!StringUtils.isEmpty(settings.getLocalDataCenter()))
			sessionBuilder = sessionBuilder.withLocalDatacenter(settings.getLocalDataCenter());
		if (settings.getPort() > -1)
			sessionBuilder = sessionBuilder.addContactPoint(new InetSocketAddress(settings.getHost(), settings.getPort()));
		if (!StringUtils.isEmpty(settings.getUsername()))
			sessionBuilder = sessionBuilder.withAuthCredentials(settings.getUsername(), settings.getPassword());
		session = sessionBuilder.build();
		started = Instant.now();
		stopped = null;
	}

	private DriverConfigLoader getConfigLoader()
	{
		return DriverConfigLoader.programmaticBuilder()
				.withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofMillis(timeout))
				.build();
	}

	public void stop() throws Exception
	{
		if (session != null)
		{
			session.close();
			session = null;
		}
		stopped = Instant.now();
	}
	
	
	public CassandraConnectionSettings getSettings()
	{
		return settings;
	}
	
	public CqlSession getSession()
	{
		return session;
	}
	
	public Instant getStarted()
	{
		return started;
	}
	
	public Instant getStopped()
	{
		return stopped;
	}
	
	public boolean isRunning()
	{
		return started != null && stopped == null;
	}
}
