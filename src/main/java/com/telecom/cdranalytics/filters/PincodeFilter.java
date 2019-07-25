package com.telecom.cdranalytics.filters;

import java.util.ArrayList;
import java.util.List;

public class PincodeFilter {

	private List<String> freePackEligiblePincode = new ArrayList<String>();
	
	public void init() {
		freePackEligiblePincode.add("560076");
		freePackEligiblePincode.add("400017");
		freePackEligiblePincode.add("400017");
		freePackEligiblePincode.add("500028");
	}
	
	public boolean isFreePackEligiblePicode(String city) {
		return freePackEligiblePincode.contains(city);
	}
}
