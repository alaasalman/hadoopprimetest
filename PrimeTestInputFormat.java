import java.util.ArrayList;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RecordReader;


public class PrimeTestInputFormat implements InputFormat<LongWritable, LongWritable>
{
    public RecordReader<LongWritable, LongWritable> getRecordReader(InputSplit split, JobConf job, Reporter reporter)
    {
        return new PrimeRecordReader((PrimeInputSplit)split);
    }

    public InputSplit[] getSplits(JobConf job, int numSplits)
    {
        long startingNumber = 2; //from 
        long endingNumber = 265; // to, exclusive

        long numbersInSplit = (long)Math.floor((endingNumber - startingNumber)/numSplits);
        long startingNumberInSplit = startingNumber;
        long endingNumberInSplit = startingNumberInSplit + numbersInSplit;
        long remainderInLastSplit = (endingNumber - startingNumber) - numSplits*numbersInSplit;

        ArrayList<PrimeInputSplit> splits = new ArrayList<PrimeInputSplit>(numSplits);

        for(int i = 0; i < numSplits - 1; i++)
        {
            splits.add(new PrimeInputSplit(startingNumberInSplit, endingNumberInSplit));
            startingNumberInSplit = endingNumberInSplit;
            endingNumberInSplit = startingNumberInSplit + numbersInSplit;
        }

        //add last split, with remainder if any
        splits.add(new PrimeInputSplit(startingNumberInSplit, endingNumberInSplit + remainderInLastSplit));

        return splits.toArray(new PrimeInputSplit[splits.size()]);
    }

    public void validateInput(JobConf conf)
    {
        //valid input since we are generating it
    }

}
