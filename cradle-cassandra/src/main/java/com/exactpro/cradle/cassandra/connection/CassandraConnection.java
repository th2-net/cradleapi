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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.*;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.Date;

import org.apache.commons.lang3.StringUtils;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;

public class CassandraConnection
{
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
		if (!StringUtils.isEmpty(settings.getLocalDataCenter()))
			sessionBuilder.withLocalDatacenter(settings.getLocalDataCenter());
		if (settings.getPort() > -1)
			sessionBuilder.addContactPoint(new InetSocketAddress(settings.getHost(), settings.getPort()));
		if (!StringUtils.isEmpty(settings.getUsername()))
			sessionBuilder.withAuthCredentials(settings.getUsername(), settings.getPassword());
		if (!StringUtils.isEmpty(settings.getCertificatePath()))
			sessionBuilder.withSslContext(createSslContext());
			
		session = sessionBuilder.build();
		started = new Date();
	}

	private SSLContext createSslContext()
			throws KeyStoreException, CertificateException, NoSuchAlgorithmException, IOException,
				   UnrecoverableKeyException, KeyManagementException
	{
		//Init keystore
		KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
		ks.load(null);
		ks.setCertificateEntry(settings.getCertificatePath(), createCertificate());

		//Init key manager
		KeyManagerFactory kmFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
		kmFactory.init(ks, settings.getCertificatePassword() == null ? null : settings.getCertificatePassword().toCharArray());

		//Init ssl context
		SSLContext sslContext = SSLContext.getInstance(settings.getSslProtocol());
		sslContext.init(kmFactory.getKeyManagers(), null, null);
		
		return sslContext;
	}

	private Certificate createCertificate() throws IOException, CertificateException
	{
		String certPath = settings.getCertificatePath();
		try (BufferedInputStream bis = new BufferedInputStream(Files.newInputStream(Paths.get(certPath).toAbsolutePath())))
		{
			CertificateFactory cf = CertificateFactory.getInstance(settings.getCertificateType());
			return cf.generateCertificate(bis);
		}
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
