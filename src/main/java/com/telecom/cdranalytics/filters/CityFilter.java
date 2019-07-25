package com.telecom.cdranalytics.filters;

import java.util.ArrayList;
import java.util.List;

public class CityFilter {

	private List<String> freePackEligibleCites = new ArrayList<String>();
	

	public void init() {
		freePackEligibleCites.add("BENGALURU");
		freePackEligibleCites.add("MUMBAI");
		freePackEligibleCites.add("BHOPAL");
		freePackEligibleCites.add("HYDARABAD");
	}
	
	public boolean isFreePackEligibleCity(String city) {
		return freePackEligibleCites.contains(city);
	}
}
