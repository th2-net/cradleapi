/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary 
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.cradle;

import java.io.Serializable;
import java.time.Instant;
import java.util.Arrays;

import com.exactpro.cradle.utils.CradleUtils;

public class StoredMessage implements Serializable
{
	private static final long serialVersionUID = 200983136307497672L;
	
	private StoredMessageId id;
	private byte[] message;
	private Direction direction;
	private String streamName;
	private Instant timestamp;
	private String reportId;
	
	public StoredMessage()
	{
	}
	
	public StoredMessage(StoredMessage copyFrom)
	{
		this.id = copyFrom.getId();
		this.message = copyFrom.getMessage();
		this.direction = copyFrom.getDirection();
		this.streamName = copyFrom.getStreamName();
		this.timestamp = copyFrom.getTimestamp();
		this.reportId = copyFrom.getReportId();
	}
	
	
	public StoredMessageId getId()
	{
		return id;
	}
	
	public void setId(StoredMessageId id)
	{
		this.id = id;
	}
	
	
	public byte[] getMessage()
	{
		return message;
	}
	
	public void setMessage(byte[] message)
	{
		this.message = message;
	}
	
	
	public Direction getDirection()
	{
		return direction;
	}
	
	public void setDirection(Direction direction)
	{
		this.direction = direction;
	}
	
	
	public String getStreamName()
	{
		return streamName;
	}
	
	public void setStreamName(String streamName)
	{
		this.streamName = streamName;
	}
	
	
	public Instant getTimestamp()
	{
		return timestamp;
	}
	
	public void setTimestamp(Instant timestamp)
	{
		this.timestamp = timestamp;
	}

	public String getReportId()
	{
		return reportId;
	}

	public void setReportId(String reportId)
	{
		this.reportId = reportId;
	}

	@Override
	public String toString()
	{
		return new StringBuilder()
				.append("StoredMessage{").append(CradleUtils.EOL)
				.append("id=").append(id).append(",").append(CradleUtils.EOL)
				.append("message=").append(Arrays.toString(message)).append(",").append(CradleUtils.EOL)
				.append("streamName='").append(streamName).append("',").append(CradleUtils.EOL)
				.append("direction='").append(direction.toString().toLowerCase()).append("',").append(CradleUtils.EOL)
				.append("timestamp='").append(timestamp).append("',").append(CradleUtils.EOL)
				.append("}").toString();
	}
}
