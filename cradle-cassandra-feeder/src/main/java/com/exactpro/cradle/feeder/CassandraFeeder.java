/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle.feeder;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;

import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.cassandra.CassandraCradleManager;
import com.exactpro.cradle.cassandra.connection.CassandraConnection;
import com.exactpro.cradle.cassandra.connection.CassandraConnectionSettings;
import com.exactpro.cradle.feeder.messages.MessagesFeeder;
import com.exactpro.cradle.utils.CradleStorageException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class CassandraFeeder implements AutoCloseable
{
	private final CradleManager manager;
	private final CradleStorage storage;
	private ObjectMapper jsonMapper;
	
	public static void main(String[] args) throws Exception
	{
		if (args.length < 2)
		{
			System.out.println("Expected parameters: <name of file with streams> <name of file with messages>");
			return;
		}
		
		PropertyConfigurator.configureAndWatch("log.properties");
		
		try (CassandraFeeder feeder = new CassandraFeeder(new File("feeder.cfg")))
		{
			feeder.feed(new File(args[0]), new File(args[1]));
		}
	}
	
	
	public CassandraFeeder(File configFile) throws FileNotFoundException, IOException, CradleStorageException
	{
		Config cfg = loadConfig(configFile);
		manager = createManager(cfg);
		storage = manager.getStorage();
		jsonMapper = createJsonMapper();
	}
	
	
	@Override
	public void close() throws Exception
	{
		if (manager != null)
			manager.dispose();
	}
	
	public void feed(File streamsFile, File messagesFile) throws IOException
	{
		try
		{
			new JsonProcessor("messages", new MessagesFeeder(jsonMapper, storage)).process(messagesFile);
		}
		catch (Exception e)
		{
			throw new IOException("Error while feeding messages", e);
		}
	}
	
	
	protected CradleManager createManager(Config cfg) throws CradleStorageException
	{
		CassandraConnectionSettings settings = new CassandraConnectionSettings(
				cfg.getDataCenter(), 
				cfg.getHost(), 
				cfg.getPort(), 
				cfg.getKeyspace());
		settings.setUsername(cfg.getUsername());
		settings.setPassword(cfg.getPassword());
		
		CradleManager result = new CassandraCradleManager(new CassandraConnection(settings));
		result.init(cfg.getInstanceName());
		return result;
	}
	
	protected ObjectMapper createJsonMapper()
	{
		ObjectMapper result = new ObjectMapper();
		result.configure(SerializationFeature.INDENT_OUTPUT, true);
		result.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
		return result;
	}
	
	
	private Config loadConfig(File config) throws FileNotFoundException, IOException
	{
		Properties props = new Properties();
		props.load(new FileInputStream(config));
		Config result = new Config();
		result.setDataCenter(props.getProperty("datacenter", ""));
		result.setHost(props.getProperty("host", ""));
		
		result.setPort(Integer.parseInt(props.getProperty("port", "-1")));
		result.setKeyspace(props.getProperty("keyspace"));
		result.setInstanceName(props.getProperty("instance", "Cradle feeder "+InetAddress.getLocalHost().getHostName()));
		
		result.setUsername(props.getProperty("username", ""));
		result.setPassword(props.getProperty("password", ""));
		return result;
	}
}
