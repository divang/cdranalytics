package com.telecom.cdranalytics.filters;

import java.util.Calendar;
import java.util.Date;

public class TimebasedFilter {
	
	private int freeHourStart = 10;
	private int freeHourEnd = 22;
	
	public boolean isBelongToFreeTimeZone(Date startTime, Date endTime) {
	
		Calendar startCalDate = Calendar.getInstance();
		startCalDate.setTime(startTime);
		
		Calendar endCalDate = Calendar.getInstance();
		endCalDate.setTime(endTime);
		
		
		if(startCalDate.get(Calendar.HOUR_OF_DAY)  >= freeHourStart && endCalDate.get(Calendar.HOUR_OF_DAY) <= freeHourEnd) {
			return true;
		}
		return false;
	}
	
}
