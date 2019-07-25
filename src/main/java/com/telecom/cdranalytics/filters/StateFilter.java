package com.telecom.cdranalytics.filters;

import java.util.ArrayList;
import java.util.List;

public class StateFilter {

private List<String> freePackEligibleState = new ArrayList<String>();
	
	public void init() {
		freePackEligibleState.add("MH");
		freePackEligibleState.add("KA");
		freePackEligibleState.add("AP");
		freePackEligibleState.add("MP");
	}
	
	public boolean isFreePackEligibleState(String city) {
		return freePackEligibleState.contains(city);
	}
}
