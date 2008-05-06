
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.RecordReader;

public class PrimeRecordReader implements RecordReader<LongWritable, LongWritable>
{
    private long m_End;
    private long m_Index;
    private long m_Start;

    public PrimeRecordReader(PrimeInputSplit split)
    {
        this.m_End = split.getEndNum();
        this.m_Index = split.getStartNum(); //index at starting number of split
        this.m_Start = split.getStartNum();
    }

    public LongWritable createKey()
    {
        return new LongWritable();
    }

    public LongWritable createValue()
    {
        return new LongWritable();
    }

    public void close(){}

    public float getProgress()
    {
        if(this.m_Index  == this.m_End)
        {
            return 0.0f;
        }
        else
        {
            return Math.min(1.0f, (this.m_Index - this.m_Start) / (float)(this.m_End - this.m_Start));
        }
    }

    public long getPos()
    {
        return this.m_End - this.m_Index;
    }

    public boolean next(LongWritable key, LongWritable value)
    {
        if(this.m_Index < this.m_End)
        {
            key.set(this.m_Index);
            value.set(this.m_Index);
            this.m_Index++;
    
            return true;
        }
        else
        {
            return false;
        }
    }
}
