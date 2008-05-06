import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class PrimeTest
{
    private static final Log LOG = LogFactory.getLog("PrimeTest");

    public static class Map extends MapReduceBase implements Mapper<LongWritable, LongWritable, BooleanWritable, LongWritable>
    {
 	    private final static BooleanWritable truePrime = new BooleanWritable(true);
 	    private final static BooleanWritable falsePrime = new BooleanWritable(false);
 	
 	    public void map(LongWritable key, LongWritable value, OutputCollector<BooleanWritable, LongWritable> output, Reporter reporter) throws IOException
        {
            long suspectPrimeValue = value.get();
            boolean isComposite = false;

            for(long i = 2; i < suspectPrimeValue - 1 ; i++)
            {
                if(suspectPrimeValue % i == 0)
                {
                    output.collect(falsePrime, value);
                    isComposite = true;
                    break;
                }
            }
            
            if(!isComposite)
            {
 	            output.collect(truePrime, value);
            }
            System.out.println(suspectPrimeValue);
        }
 	}
 	
 	public static class Reduce extends MapReduceBase implements Reducer<BooleanWritable, LongWritable, BooleanWritable, LongWritable>
    {
 	    public void reduce(BooleanWritable key, Iterator<LongWritable> values, OutputCollector<BooleanWritable, LongWritable> output, Reporter reporter) throws IOException
        {
            while(values.hasNext())
            {
                LongWritable vCheck = values.next();
                output.collect(key, vCheck);
            }
 	    
 	    }

 	}
 	
 	public static void main(String[] args) throws Exception
    {
 	    JobConf conf = new JobConf(PrimeTest.class);
 	    conf.setJobName("primetest");
 	
 	    conf.setOutputKeyClass(BooleanWritable.class);
 	    conf.setOutputValueClass(LongWritable.class);
 	
 	    conf.setMapperClass(Map.class);
 	    conf.setCombinerClass(Reduce.class);
 	    conf.setReducerClass(Reduce.class);
 	
 	    conf.setInputFormat(PrimeTestInputFormat.class);
 	    conf.setOutputFormat(TextOutputFormat.class);
 	
 	    conf.setOutputPath(new Path(args[0]));
 	
 	    JobClient.runJob(conf);
 	}
}
