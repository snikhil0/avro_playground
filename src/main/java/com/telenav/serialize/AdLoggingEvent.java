package com.telenav.serialize;

import java.util.Map;

import org.apache.log4j.Category;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LocationInfo;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.spi.ThrowableInformation;

public class AdLoggingEvent extends LoggingEvent {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public AdLoggingEvent(String fqnOfCategoryClass, Category logger,
			long timeStamp, Level level, Object message, String threadName,
			ThrowableInformation throwable, String ndc, LocationInfo info,
			Map properties) {
		super(fqnOfCategoryClass, logger, timeStamp, level, message, threadName,
				throwable, ndc, info, properties);
		// TODO Auto-generated constructor stub
	}

}
