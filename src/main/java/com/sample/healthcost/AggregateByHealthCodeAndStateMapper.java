package com.sample.healthcost;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AggregateByHealthCodeAndStateMapper extends
		Mapper<Object, Text, Text, Text> {

	private Text keyStr = new Text();

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] csv = value.toString().split("\t");

		if (!csv[0].equalsIgnoreCase("npi")) {
			keyStr.set(csv[16]+"~"+csv[11]);
			context.write(keyStr, value);
		}
	}

}
