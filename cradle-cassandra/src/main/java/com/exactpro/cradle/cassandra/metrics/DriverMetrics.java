/*
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.cradle.cassandra.metrics;

import com.datastax.oss.driver.api.core.CqlSession;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.dropwizard.samplebuilder.CustomMappingSampleBuilder;
import io.prometheus.client.dropwizard.samplebuilder.MapperConfig;

import java.util.*;

import static java.lang.String.format;

public class DriverMetrics
{
	// Session level metrics. All of them has prefix "session_".
	// The number and rate of bytes sent for the entire session (exposed as a Meter).
	private static final String SESSION_BYTES_SENT = "bytes-sent";
	// The number and rate of bytes received for the entire session (exposed as a Meter).
	private static final String SESSION_BYTES_RECEIVED = "bytes-received";
	// The number of nodes to which the driver has at least one active connection (exposed as a
	// Gauge<Integer>).
	private static final String CONNECTED_NODES = "connected-nodes";
	// The throughput and latency percentiles of CQL requests (exposed as a Timer).
	private static final String CQL_REQUESTS = "cql-requests";
	// The number of CQL requests that timed out -- that is, the session.execute() call failed
	// with a DriverTimeoutException (exposed as a Counter).
	private static final String CQL_CLIENT_TIMEOUTS = "cql-client-timeouts";
	// The size of the driver-side cache of CQL prepared statements.
	private static final String CQL_PREPARED_CACHE_SIZE = "cql-prepared-cache-size";
	// How long requests are being throttled (exposed as a Timer).
	private static final String THROTTLING_DELAY = "throttling.delay";
	// The size of the throttling queue (exposed as a Gauge<Integer>).
	private static final String THROTTLING_QUEUE_SIZE = "throttling.queue-size";
	// The number of times a request was rejected with a RequestThrottlingException (exposed as
	// a Counter)
	private static final String THROTTLING_ERRORS = "throttling.errors";

	private static final String[] SESSION_LEVEL_METRICS = {SESSION_BYTES_SENT, SESSION_BYTES_RECEIVED, CONNECTED_NODES,
			CQL_REQUESTS, CQL_CLIENT_TIMEOUTS, CQL_PREPARED_CACHE_SIZE, THROTTLING_DELAY, THROTTLING_QUEUE_SIZE, THROTTLING_ERRORS};
	private static final String SESSION_LEVEL_METRICS_FORMATTER = "*.%s";
	private static final String[] SESSION_LEVEL_METRICS_PARAMS = {"session"};

	// Node level metrics. All of them has prefix "node_".
	// The number of connections open to this node for regular requests (exposed as a Gauge<Integer>)
	private static final String POOL_OPEN_CONNECTIONS = "pool.open-connections";
	// The number of stream ids available on the connections to this node (exposed as a Gauge<Integer>).
	private static final String POOL_AVAILABLE_STREAMS = "pool.available-streams";
	// The number of requests currently executing on the connections to this node (exposed as a
	// Gauge<Integer>). This includes orphaned streams.
	private static final String POOL_IN_FLIGHT = "pool.in-flight";
	// The number of "orphaned" stream ids on the connections to this node (exposed as a Gauge<Integer>).
	private static final String POOL_ORPHANED_STREAMS = "pool.orphaned-streams";
	// The number and rate of bytes sent to this node (exposed as a Meter).
	private static final String NODE_BYTES_SENT = "bytes-sent";
	// The number and rate of bytes received from this node (exposed as a Meter).
	private static final String NODE_BYTES_RECEIVED = "bytes-received";
	// The throughput and latency percentiles of individual CQL messages sent to this node as
	// part of an overall request (exposed as a Timer).
	private static final String CQL_MESSAGES = "cql-messages";
	// The number of times the driver failed to send a request to this node (exposed as a Counter).
	private static final String ERRORS_REQUEST_UNSENT = "errors.request.unsent";
	// The number of times a request was aborted before the driver even received a response
	// from this node (exposed as a Counter).
	private static final String ERRORS_REQUEST_ABORTED = "errors.request.aborted";
	// The number of times this node replied with a WRITE_TIMEOUT error (exposed as a Counter).
	private static final String ERRORS_REQUEST_WRITE_TIMEOUT = "errors.request.write-timeouts";
	// The number of times this node replied with a READ_TIMEOUT error (exposed as a Counter).
	private static final String ERRORS_REQUEST_READ_TIMEOUT = "errors.request.read-timeouts";
	// The number of times this node replied with an UNAVAILABLE error (exposed as a Counter).
	private static final String ERRORS_REQUEST_UNAVAILABLE = "errors.request.unavailables";
	// The number of times this node replied with an error that doesn't fall under other
	// 'errors.*' metrics (exposed as a Counter).
	private static final String ERRORS_REQUEST_OTHERS = "errors.request.others";
	// The total number of errors on this node that caused the RetryPolicy to trigger a retry (exposed as a Counter).
	private static final String RETRIES_TOTAL = "retries.total";
	// The number of errors on this node that caused the RetryPolicy to trigger a retry, broken
    // down by error type (exposed as Counters).
	private static final String RETRIES_ABORTED = "retries.aborted";
	private static final String RETRIES_READ_TIMEOUT = "retries.read-timeout";
	private static final String RETRIES_WRITE_TIMEOUT = "retries.write-timeout";
	private static final String RETRIES_UNAVAILABLE = "retries.unavailable";
	private static final String RETRIES_OTHER = "retries.other";
	// The total number of errors on this node that were ignored by the RetryPolicy (exposed as a Counter).
	private static final String IGNORES_TOTAL = "ignores.total";
	// The number of errors on this node that were ignored by the RetryPolicy, broken down by
	// error type (exposed as Counters).
	private static final String IGNORES_ABORTED = "ignores.aborted";
	private static final String IGNORES_READ_TIMEOUT = "ignores.read-timeout";
	private static final String IGNORES_WRITE_TIMEOUT = "ignores.write-timeout";
	private static final String IGNORES_UNAVIABLE = "ignores.unavailable";
	private static final String IGNORES_OTHER = "ignores.other";
	// The number of speculative executions triggered by a slow response from this node (exposed as a Counter).
	private static final String SPECULATIVE_EXECUTIONS = "speculative-executions";
	// The number of errors encountered while trying to establish a connection to this node (exposed as a Counter).
	private static final String ERRORS_CONNECTION_INIT = "errors.connection.init";
	// The number of authentication errors encountered while trying to establish a connection
	// to this node (exposed as a Counter).
	private static final String ERRORS_CONNECTION_AUTH = "errors.connection.auth";

	private static final String[] NODE_LEVEL_METRICS = {POOL_OPEN_CONNECTIONS, POOL_AVAILABLE_STREAMS, POOL_IN_FLIGHT,
			POOL_ORPHANED_STREAMS, NODE_BYTES_SENT, NODE_BYTES_RECEIVED, CQL_MESSAGES, ERRORS_REQUEST_UNSENT,
			ERRORS_REQUEST_ABORTED, ERRORS_REQUEST_WRITE_TIMEOUT, ERRORS_REQUEST_READ_TIMEOUT, ERRORS_REQUEST_UNAVAILABLE,
			ERRORS_REQUEST_OTHERS, RETRIES_TOTAL, RETRIES_ABORTED, RETRIES_READ_TIMEOUT, RETRIES_WRITE_TIMEOUT,
			RETRIES_UNAVAILABLE, RETRIES_OTHER, IGNORES_TOTAL, IGNORES_ABORTED, IGNORES_READ_TIMEOUT, IGNORES_WRITE_TIMEOUT,
			IGNORES_UNAVIABLE, IGNORES_OTHER, SPECULATIVE_EXECUTIONS, ERRORS_CONNECTION_INIT, ERRORS_CONNECTION_AUTH};
	private static final String NODE_LEVEL_METRICS_FORMATTER = "*.nodes.*.%s";
	private static final String[] NODE_LEVEL_METRICS_PARAMS = {"session", "node"};

	public static void register(CqlSession session)
	{
		session.getMetrics().ifPresent(metrics -> new DropwizardExports(metrics.getRegistry(),
				new CustomMappingSampleBuilder(createMapperConfigs())).register());
	}
	
	private static List<MapperConfig> createMapperConfigs()
	{
		List<MapperConfig> result = new ArrayList<>();
		// Add session level metrics
		for (String metricName : SESSION_LEVEL_METRICS)
		{
			result.add(new MapperConfig(format(SESSION_LEVEL_METRICS_FORMATTER, metricName), format("session_%s", metricName),
					generateLabels(SESSION_LEVEL_METRICS_PARAMS)));
		}
		// Add session level metrics
		for (String metricName : NODE_LEVEL_METRICS)
		{
			result.add(new MapperConfig(format(NODE_LEVEL_METRICS_FORMATTER, metricName), format("node_%s", metricName),
					generateLabels(NODE_LEVEL_METRICS_PARAMS)));
		}

		return result;
	}
	
	private static Map<String, String> generateLabels(String[] labels)
	{
		Map<String, String> result = new LinkedHashMap<>();
		int index = 0;
		for (String label : labels)
			result.put(label, format("${%d}", index++));

		return result;
	}
}
