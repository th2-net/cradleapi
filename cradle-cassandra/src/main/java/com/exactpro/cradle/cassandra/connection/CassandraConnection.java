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
import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultProgrammaticDriverConfigLoaderBuilder;
import com.exactpro.cradle.cassandra.CassandraStorageSettings;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.lang3.StringUtils;

public class CassandraConnection
{
	public static final String DRIVER_CONFIG_FILE_NAME = "application.conf";
	private static final Path DRIVER_CONFIG = Paths.get(System.getProperty("user.dir"), DRIVER_CONFIG_FILE_NAME);
	
	private final CassandraConnectionSettings connectionSettings;
	private final CassandraStorageSettings storageSettings;
	private volatile CqlSession session;
	private volatile Instant started,
			stopped;

	public CassandraConnection()
	{
		this.connectionSettings = createConnectionSettings();
		this.storageSettings = createStorageSettings();
	}

	public CassandraConnection(CassandraConnectionSettings connectionSettings, CassandraStorageSettings storageSettings)
	{
		this.connectionSettings = connectionSettings;
		this.storageSettings = storageSettings;
	}

	
	public void start() throws Exception
	{
		CqlSessionBuilder sessionBuilder = CqlSession.builder();
		sessionBuilder.withConfigLoader(getConfigLoader());
		if (!StringUtils.isEmpty(connectionSettings.getLocalDataCenter()))
			sessionBuilder = sessionBuilder.withLocalDatacenter(connectionSettings.getLocalDataCenter());
		if (connectionSettings.getPort() > -1)
			sessionBuilder = sessionBuilder.addContactPoint(new InetSocketAddress(connectionSettings.getHost(), connectionSettings
					.getPort()));
		if (!StringUtils.isEmpty(connectionSettings.getUsername()))
			sessionBuilder = sessionBuilder.withAuthCredentials(
					connectionSettings.getUsername(), connectionSettings.getPassword());
		session = sessionBuilder.build();
		started = Instant.now();
		stopped = null;
	}

	private DriverConfigLoader getConfigLoader()
	{
		Supplier<Config> fallBackSupplier = () -> {
			Config config = ConfigFactory.defaultApplication();
			if (Files.exists(DRIVER_CONFIG))
				config = config.withFallback(ConfigFactory.parseFileAnySyntax(DRIVER_CONFIG.toFile()));
			return config.withFallback(ConfigFactory.defaultReference()).resolve();
		};

		return new DefaultProgrammaticDriverConfigLoaderBuilder(fallBackSupplier, DefaultDriverConfigLoader.DEFAULT_ROOT_PATH)
				//Set the init-query timeout the same as for the select query
				.withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT,
						Duration.ofMillis(getQueryTimeout()))
				.build();
	}

	private long getQueryTimeout()
	{
		long queryTimeout = storageSettings.getTimeout();
		return queryTimeout == 0 ? storageSettings.DEFAULT_TIMEOUT : queryTimeout;
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
	
	
	public CassandraConnectionSettings getConnectionSettings()
	{
		return connectionSettings;
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
	
	
	protected CassandraConnectionSettings createConnectionSettings()
	{
		return new CassandraConnectionSettings();
	}
	
	protected CassandraStorageSettings createStorageSettings()
	{
		return new CassandraStorageSettings();
	}
}
