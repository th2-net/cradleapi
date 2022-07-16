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
import java.util.Date;
import java.util.function.Supplier;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultProgrammaticDriverConfigLoaderBuilder;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.lang3.StringUtils;

import static com.exactpro.cradle.cassandra.CassandraStorageSettings.DEFAULT_TIMEOUT;

public class CassandraConnection
{
	public static final String DRIVER_CONFIG_FILE_NAME = "application.conf";
	private static final Path DRIVER_CONFIG = Paths.get(System.getProperty("user.dir"), DRIVER_CONFIG_FILE_NAME);
	private CassandraConnectionSettings settings;
	private CqlSession session;
	private Date started,
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
		sessionBuilder.withConfigLoader(getConfigLoader());
		if (!StringUtils.isEmpty(settings.getLocalDataCenter()))
			sessionBuilder = sessionBuilder.withLocalDatacenter(settings.getLocalDataCenter());
		if (settings.getPort() > -1)
			sessionBuilder = sessionBuilder.addContactPoint(new InetSocketAddress(settings.getHost(), settings.getPort()));
		if (!StringUtils.isEmpty(settings.getUsername()))
			sessionBuilder = sessionBuilder.withAuthCredentials(settings.getUsername(), settings.getPassword());
		session = sessionBuilder.build();
		started = new Date();
	}

	private DriverConfigLoader getConfigLoader()
	{
		Supplier<Config> fallBackSupplier = () -> {
			Config config = ConfigFactory.defaultApplication();
			if (Files.exists(DRIVER_CONFIG))
				config = config.withFallback(ConfigFactory.parseFileAnySyntax(DRIVER_CONFIG.toFile()));
			return config.withFallback(ConfigFactory.defaultReference()).resolve();
		};

		long queryTimeout = settings.getTimeout() == 0 ? DEFAULT_TIMEOUT : settings.getTimeout();
		return new DefaultProgrammaticDriverConfigLoaderBuilder(fallBackSupplier, DefaultDriverConfigLoader.DEFAULT_ROOT_PATH)
				//Set the init-query timeout the same as for the select query
				.withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofMillis(queryTimeout))
				.build();
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
