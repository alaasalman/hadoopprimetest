
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.mapred.InputSplit;

public class PrimeInputSplit implements InputSplit
{
    private long m_StartNum;
    private long m_EndNum;

    PrimeInputSplit(){}

    public PrimeInputSplit(long p_Start, long p_End)
    {
        this.m_StartNum = p_Start;
        this.m_EndNum = p_End;
    }

    public long getLength()
    {
        return (this.m_EndNum - this.m_StartNum) * 8;
    }

    public String[] getLocations() throws IOException
    {
        return new String[]{};
    }

    public void readFields(DataInput in) throws IOException
    {
        this.m_StartNum = in.readLong();
        this.m_EndNum = in.readLong();
    }

    public void write(DataOutput out) throws IOException
    {
        out.writeLong(this.m_StartNum);
        out.writeLong(this.m_EndNum);
    }

    public long getStartNum()
    {
        return this.m_StartNum;
    }

    public long getEndNum()
    {
        return this.m_EndNum;
    }


}
