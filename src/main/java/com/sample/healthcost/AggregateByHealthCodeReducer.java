package com.sample.healthcost;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AggregateByHealthCodeReducer extends
		Reducer<Text, Text, Text, Text> {

	public void reduce(Text text, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		String code = "";
		String codeDesc = "";
		double lowestPayment = 0;
		double highestPayment = 0;
		double totalPayment = 0;
		int totalCount = 0;

		for (Text value : values) {
			try {
				String[] csv = value.toString().split("\t");
				double localPmt = Double.parseDouble(csv[25]);
				totalCount++;
				if (lowestPayment == 0 || lowestPayment > localPmt)
					lowestPayment = localPmt;
				if (highestPayment == 0 || highestPayment < localPmt)
					highestPayment = localPmt;
				totalPayment = totalPayment + localPmt;
				codeDesc = csv[17];
				code = csv[16];
			} catch (NumberFormatException exception) {
				continue;
			}
		}

		context.write(text, new Text(text.toString() + "," + code + ","
				+ codeDesc + "," + (totalPayment / totalCount) + ","
				+ lowestPayment + "," + highestPayment));

	}

}
