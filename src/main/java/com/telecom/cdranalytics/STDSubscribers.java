package com.telecom.cdranalytics;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.telecom.cdranalytics.filters.CityFilter;
import com.telecom.cdranalytics.filters.PincodeFilter;
import com.telecom.cdranalytics.filters.StateFilter;
import com.telecom.cdranalytics.filters.TimebasedFilter;

public class STDSubscribers {

	private static String targetTable = "promotedFreePackUsers";

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		if (args.length != 1) {
			//System.err.println("Usage: stdsubscriber <in> <out>");
			System.err.println("Usage: stdsubscriber <in>");
			System.exit(2);
		}
		Job job = new Job(conf, "STD Subscribers");
		job.setJarByClass(STDSubscribers.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));

		Scan scan = new Scan();
		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);  // don't set to true for MR jobs

		TableMapReduceUtil.initTableReducerJob(
				targetTable,        // output table
				PromotedFreeUserReducer.class,    // reducer class
				job);
		job.setNumReduceTasks(1);   // at least one, adjust as required


		//	FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class TokenizerMapper extends
	Mapper<Object, Text, Text, LongWritable> {

		Text phoneNumber = new Text();
		LongWritable durationInMinutes = new LongWritable();
		TimebasedFilter timebasedFilter = new TimebasedFilter();
		StateFilter stateFilter = new StateFilter();
		PincodeFilter pincodeFilter = new PincodeFilter();
		CityFilter cityFilter = new CityFilter();
		
		private long toMillis(String date) {

			SimpleDateFormat format = new SimpleDateFormat(
					"yyyy-MM-dd HH:mm:ss");
			Date dateFrm = null;
			try {
				dateFrm = format.parse(date);

			} catch (ParseException e) {

				e.printStackTrace();
			}
			return dateFrm.getTime();
		}
		
		private Date toDate(String date) {

			SimpleDateFormat format = new SimpleDateFormat(
					"yyyy-MM-dd HH:mm:ss");
			Date dateFrm = null;
			try {
				dateFrm = format.parse(date);
				
			} catch (ParseException e) {

				e.printStackTrace();
			}
			return dateFrm;
		}
		
		
		public void map(Object key, Text value,
				Mapper<Object, Text, Text, LongWritable>.Context context)
						throws IOException, InterruptedException {
			String[] parts = value.toString().split("[|]");
			if (parts[CDRConstants.STDFlag].equalsIgnoreCase("1")) {

				if(stateFilter.isFreePackEligibleState(parts[CDRConstants.callerState])) {
				
					if(cityFilter.isFreePackEligibleCity(parts[CDRConstants.callerCity])) {
						
						if(pincodeFilter.isFreePackEligiblePicode(parts[CDRConstants.callerPincode])) {
							phoneNumber.set(parts[CDRConstants.fromPhoneNumber]);
							String callEndTime = parts[CDRConstants.callEndTime];
							String callStartTime = parts[CDRConstants.callStartTime];
							Date startDate = toDate(callStartTime);
							Date endDate = toDate(callEndTime);
							
							if(timebasedFilter.isBelongToFreeTimeZone(startDate, endDate)) {
								long duration = toMillis(callEndTime) - toMillis(callStartTime);
								durationInMinutes.set(duration / (1000 * 60));
								context.write(phoneNumber, durationInMinutes);
							} 
							else {
								System.out.println("Time range is not in Free pack zone");
							}
						}
						else {
							System.out.println("Area of the city (pincode) is not in Free pack zone");
						}
					}
					else {
						System.out.println("City is not in Free pack zone");
					}
				}
				else {
					System.out.println("State is not in Free pack zone");
				}
			}
			else {
				System.out.println("Local call is not in Free pack");
			}
		}
	}

	public static class PromotedFreeUserReducer extends TableReducer<Text, LongWritable, ImmutableBytesWritable>  {
		public static final byte[] CF = "callerStatistic".getBytes();
		public static final byte[] COUNT = "duration".getBytes();

		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long totaldDuration = 0;
			for (LongWritable val : values) {
				totaldDuration += val.get();
			}
			Put put = new Put(Bytes.toBytes(key.toString()));
			put.addColumn(CF, COUNT, Bytes.toBytes(totaldDuration));

			context.write(null, put);
		}
	}

}
