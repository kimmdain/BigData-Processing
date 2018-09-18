import java.util.*;
import java.io.*;
import java.text.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class UBERbyDainKim {
	 public static class Map extends Mapper<LongWritable, Text, Text, Text>
	{
	private Text word = new Text(); 
	private Text wordValue = new Text();
	String keyStr = "";
	String valueStr = "";

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

  		String line = value.toString();
		DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		Date date = new Date();
		Calendar calendar;
  		
		StringTokenizer tokenizer = new StringTokenizer(line, ",");
		while(tokenizer.hasMoreTokens()) {
			String inputDate="";
			keyStr = tokenizer.nextToken()+",";
			inputDate = tokenizer.nextToken().toString();
			valueStr = tokenizer.nextToken()+","+tokenizer.nextToken();
 			try { 
				date = dateFormat.parse(inputDate);
			} catch (ParseException pe) {}
			calendar = Calendar.getInstance();
			calendar.setTime(date);

			int dayNum = calendar.get(Calendar.DAY_OF_WEEK);
			String day ="";
       			switch(dayNum){
        		case 1:
        	    		day = "SUN";
        	    		break;
        		case 2:
            			day = "MON";
            			break;
        		case 3:
            			day = "TUE";
            			break;
        		case 4:
            			day = "WED";
            			break;
        		case 5:
            			day = "THU";
           			break;
        		case 6:
            			day = "FRI";
            			break;
        		case 7:
            			day = "SAT";
            			break;
        		}
 		
			keyStr += day;
			word.set(keyStr);
			wordValue.set(valueStr);
			context.write(word, wordValue);
			}
  		} 
  
	}

	public static class Reduce extends Reducer<Text,Text, Text, Text>
	{
		private Text wordValue = new Text();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			long sumTrips = 0; 
			long sumVehicles = 0;
			for (Text val : values) {
			StringTokenizer tokenizer = new StringTokenizer(val.toString(), ",");
	
			sumTrips += Integer.parseInt(tokenizer.nextToken());
			sumVehicles += Integer.parseInt(tokenizer.nextToken());	
			}
			wordValue.set(sumVehicles+","+sumTrips);
   			context.write(key, wordValue);
  		}
 	}

	public static void main(String[] args) throws Exception
 	{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "UBERStudent20150968");
  
		job.setJarByClass(UBERStudent20150968.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
  
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
 
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileSystem.get(job.getConfiguration()).delete(new Path(args[1]),true);
  		job.waitForCompletion(true);
 	}
}
