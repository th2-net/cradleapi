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

import java.io.IOException;
import java.util.List;

public interface ReportsMessagesLinker
{
	/**
	 * Retrieves ID of stored report by linked message ID
	 * @param messageId ID of stored message
	 * @return ID of stored report
	 * @throws IOException if report data retrieval failed
	 */
	String getReportIdByMessageId(StoredMessageId messageId) throws IOException;

	/**
	 * Retrieves IDs of stored messages by linked report ID
	 * @param reportId ID of stored report
	 * @return list of stored message IDs
	 * @throws IOException if messages data retrieval failed
	 */
	List<StoredMessageId> getMessageIdsByReportId(String reportId) throws IOException;

	/**
	 * Checks if report has messages linked to it
	 * @param reportId ID of stored report
	 * @return true if report has linked messages, false otherwise
	 * @throws IOException if messages data retrieval failed
	 */
	boolean isReportLinkedToMessages(String reportId) throws IOException;
}
