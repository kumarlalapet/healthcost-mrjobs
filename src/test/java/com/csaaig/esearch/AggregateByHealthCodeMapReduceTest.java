package com.csaaig.esearch;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.sample.healthcost.AggregateByHealthCodeMapper;
import com.sample.healthcost.AggregateByHealthCodeReducer;

public class AggregateByHealthCodeMapReduceTest {

	/** Sample records from the file
	npi	nppes_provider_last_org_name	nppes_provider_first_name	nppes_provider_mi	nppes_credentials	nppes_provider_gender	nppes_entity_code	nppes_provider_street1	nppes_provider_street2	nppes_provider_city	nppes_provider_zip	nppes_provider_state	nppes_provider_country	provider_type	medicare_participation_indicator	place_of_service	hcpcs_code	hcpcs_description	line_srvc_cnt	bene_unique_cnt	bene_day_srvc_cnt	average_Medicare_allowed_amt	stdev_Medicare_allowed_amt	average_submitted_chrg_amt	stdev_submitted_chrg_amt	average_Medicare_payment_amt	stdev_Medicare_payment_amt
	1003000126	ENKESHAFI	ARDALAN		M.D.	M	I	900 SETON DR		CUMBERLAND	215021854	MD	US	Internal Medicine	Y	F	99222	Initial hospital care	115	112	115	135.25	0	199	0	108.11565217	0.9005883395
	1003000126	ENKESHAFI	ARDALAN		M.D.	M	I	900 SETON DR		CUMBERLAND	215021854	MD	US	Internal Medicine	Y	F	99223	Initial hospital care	93	88	93	198.59	0	291	9.5916630466	158.87	0
	1003000126	ENKESHAFI	ARDALAN		M.D.	M	I	900 SETON DR		CUMBERLAND	215021854	MD	US	Internal Medicine	Y	F	99231	Subsequent hospital care	111	83	111	38.75	0	58	0	30.720720721	2.9291057922
	**/
	
	MapDriver<Object, Text, Text, Text> mapDriver;
	ReduceDriver<Text, Text, Text, Text> reduceDriver;

	@Before
	public void setUp() {
		mapDriver = MapDriver.newMapDriver();
		mapDriver.setMapper(new AggregateByHealthCodeMapper());
		reduceDriver = ReduceDriver.newReduceDriver();
		reduceDriver.setReducer(new AggregateByHealthCodeReducer());
	}

	@Test
	public void testMapper() {
		mapDriver.withInput(new Object(), new Text("npi	nppes_provider_last_org_name	nppes_provider_first_name	nppes_provider_mi	nppes_credentials	nppes_provider_gender	nppes_entity_code	nppes_provider_street1	nppes_provider_street2	nppes_provider_city	nppes_provider_zip	nppes_provider_state	nppes_provider_country	provider_type	medicare_participation_indicator	place_of_service	hcpcs_code	hcpcs_description	line_srvc_cnt	bene_unique_cnt	bene_day_srvc_cnt	average_Medicare_allowed_amt	stdev_Medicare_allowed_amt	average_submitted_chrg_amt	stdev_submitted_chrg_amt	average_Medicare_payment_amt	stdev_Medicare_payment_amt"));
		mapDriver.withInput(new Object(), new Text("1003000126	ENKESHAFI	ARDALAN		M.D.	M	I	900 SETON DR		CUMBERLAND	215021854	MD	US	Internal Medicine	Y	F	99222	Initial hospital care	115	112	115	135.25	0	199	0	108.11565217	0.9005883395"));
		 
		mapDriver.withOutput(new Text("99222"), new Text("1003000126	ENKESHAFI	ARDALAN		M.D.	M	I	900 SETON DR		CUMBERLAND	215021854	MD	US	Internal Medicine	Y	F	99222	Initial hospital care	115	112	115	135.25	0	199	0	108.11565217	0.9005883395"));
		mapDriver.runTest();
	}

	@Test
	public void testReducer() {
		List<Text> values = new ArrayList<Text>();
		values.add(new Text("1003000126	ENKESHAFI	ARDALAN		M.D.	M	I	900 SETON DR		CUMBERLAND	215021854	MD	US	Internal Medicine	Y	F	99222	Initial hospital care	115	112	115	135.25	0	199	0	108.11565217	0.9005883395"));
		values.add(new Text("1003000126	ENKESHAFI	ARDALAN		M.D.	M	I	900 SETON DR		CUMBERLAND	215021854	MD	US	Internal Medicine	Y	F	99222	Initial hospital care	93	88	93	198.59	0	291	9.5916630466	158.87	0"));
		reduceDriver.withInput(new Text("99222"), values);
		reduceDriver.withOutput(new Text("99222"), new Text("99222,99222,Initial hospital care,133.492826085,108.11565217,158.87"));
		reduceDriver.runTest();
	}
	
	@Test
	public void testReducer_WithNonNumericValueForPayment() {
		List<Text> values = new ArrayList<Text>();
		values.add(new Text("1003000126	ENKESHAFI	ARDALAN		M.D.	M	I	900 SETON DR		CUMBERLAND	215021854	MD	US	Internal Medicine	Y	F	99222	Initial hospital care	115	112	115	135.25	0	199	0	108.11565217	0.9005883395"));
		values.add(new Text("1003000126	ENKESHAFI	ARDALAN		M.D.	M	I	900 SETON DR		CUMBERLAND	215021854	MD	US	Internal Medicine	Y	F	99222	Initial hospital care	93	88	93	198.59	0	291	9.5916630466	158.87	0"));
		values.add(new Text("1003000126	ENKESHAFI	ARDALAN		M.D.	M	I	900 SETON DR		CUMBERLAND	215021854	MD	US	Internal Medicine	Y	F	99222	Initial hospital care	93	88	93	198.59	0	291	9.5916630466	Nan	0"));		
		reduceDriver.withInput(new Text("99222"), values);
		reduceDriver.withOutput(new Text("99222"), new Text("99222,99222,Initial hospital care,133.492826085,108.11565217,158.87"));
		reduceDriver.runTest();		
	}
}
