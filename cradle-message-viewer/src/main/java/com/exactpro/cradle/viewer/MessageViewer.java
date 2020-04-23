/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle.viewer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.PropertyConfigurator;

import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.cassandra.CassandraCradleManager;
import com.exactpro.cradle.cassandra.connection.CassandraConnection;
import com.exactpro.cradle.cassandra.connection.CassandraConnectionSettings;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageFilterBuilder;
import com.exactpro.cradle.utils.CradleStorageException;

public class MessageViewer implements AutoCloseable
{
	private static final String PARAM_FROM = "from",
			PARAM_TO = "to";
	
	private final CradleManager manager;
	private final CradleStorage storage;
	
	public static void main(String[] args) throws Exception
	{
		Option from = Option.builder(PARAM_FROM).required(false).hasArg().desc("Boundary for messages timestamp").build(),
				to = Option.builder(PARAM_TO).required(false).hasArg().desc("Boundary for messages timestamp").build();
		
		Options options = new Options();
		options.addOption(from);
		options.addOption(to);

		CommandLineParser parser = new DefaultParser();
		CommandLine line = null;
		try
		{
			// parse the command line arguments
			line = parser.parse(options, args);
		}
		catch (ParseException exp)
		{
			// oops, something went wrong
			System.err.println("Incorrect parameters: " + exp.getMessage());
			return;
		}
		
//		if (line.getOptions().length == 0)
//		{
//			System.err.println("At least one parameter - "+PARAM_FROM+" or "+PARAM_TO+" - should be specified");
//			return;
//		}
		
		String fromS = line.getOptionValue(from.getOpt()),
				toS = line.getOptionValue(to.getOpt());
		
		Instant timestampFrom = fromS != null ? Instant.parse(fromS) : null,
				timestampTo = toS != null ? Instant.parse(toS) : null;
		
		PropertyConfigurator.configureAndWatch("log.properties");
		
		try (MessageViewer viewer = new MessageViewer(new File("message_viewer.cfg")))
		{
			viewer.printMessages(timestampFrom, timestampTo);
		}
	}
	
	
	public MessageViewer(File configFile) throws FileNotFoundException, IOException, CradleStorageException
	{
		Config cfg = loadConfig(configFile);
		manager = createManager(cfg);
		storage = manager.getStorage();
	}
	
	
	@Override
	public void close() throws Exception
	{
		if (manager != null)
			manager.dispose();
	}
	
	public void printMessages(Instant from, Instant to) throws IOException
	{
		StoredMessageFilterBuilder filterBuilder = new StoredMessageFilterBuilder();
		if (from != null)
			filterBuilder = filterBuilder.timestampFrom().isGreaterThanOrEqualTo(from);
		if (to != null)
			filterBuilder = filterBuilder.timestampTo().isLessThanOrEqualTo(to);
		
		boolean first = true;
		for (StoredMessage msg : storage.getMessages(filterBuilder.build()))
		{
			if (first)
				first = false;
			else
			{
				System.out.println("");
				System.out.println("");
			}
			
			System.out.println("ID: "+msg.getId()+", "+msg.getDirection().getLabel()+" via "+msg.getStreamName()+" at "+msg.getTimestamp());
			System.out.println("\t"+new String(msg.getContent(), StandardCharsets.UTF_8).replace((char)1, '|').replaceAll("\\p{C}", "."));
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
	
	private Config loadConfig(File config) throws FileNotFoundException, IOException
	{
		Properties props = new Properties();
		props.load(new FileInputStream(config));
		Config result = new Config();
		result.setDataCenter(props.getProperty("datacenter", ""));
		result.setHost(props.getProperty("host", ""));
		
		result.setPort(Integer.parseInt(props.getProperty("port", "-1")));
		result.setKeyspace(props.getProperty("keyspace"));
		result.setInstanceName(props.getProperty("instance", ""));
		
		result.setUsername(props.getProperty("username", ""));
		result.setPassword(props.getProperty("password", ""));
		return result;
	}
}
