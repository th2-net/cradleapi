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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;

import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import org.apache.commons.lang3.StringUtils;

public class CassandraConnection
{
	public static final String DRIVER_CONFIG_FILE_NAME = "application.conf";
	private static final Path DRIVER_CONFIG = Paths.get(System.getProperty("user.dir"), DRIVER_CONFIG_FILE_NAME);
	
	private final CassandraConnectionSettings settings;
	private volatile CqlSession session;
	private volatile Instant started,
			stopped;

	public CassandraConnection()
	{
		this.settings = createSettings();
	}

	public CassandraConnection(CassandraConnectionSettings settings)
	{
		this.settings = settings;
	}

	
	public void start() throws Exception
	{
		CqlSessionBuilder sessionBuilder = CqlSession.builder();
		if (Files.exists(DRIVER_CONFIG))
			sessionBuilder.withConfigLoader(DriverConfigLoader.fromFile(DRIVER_CONFIG.toFile()));
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
	
	
	protected CassandraConnectionSettings createSettings()
	{
		return new CassandraConnectionSettings();
	}
}
