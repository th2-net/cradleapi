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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;

import java.net.InetSocketAddress;
import java.util.Date;

import org.apache.commons.lang.StringUtils;

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
