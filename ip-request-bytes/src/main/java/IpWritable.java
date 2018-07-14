import com.google.common.base.Objects;
import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * The IpWritable represents the information about the IP connection. It contains the average count
 * of bytes and total bytes.
 */
public class IpWritable implements Writable, WritableComparable<IpWritable> {
    private FloatWritable avgBytes;
    private LongWritable bytes;

    public IpWritable() {
        this.avgBytes = new FloatWritable(0);
        this.bytes = new LongWritable(0);
    }

    public IpWritable(FloatWritable avgBytes, LongWritable bytes) {
        this.avgBytes = avgBytes;
        this.bytes = bytes;
    }

    public void getIp(float avgBytes, String bytes) {
        this.avgBytes.set(avgBytes);
        this.bytes.set(Long.valueOf(bytes));
    }

    public FloatWritable getAvgBytes() {
        return avgBytes;
    }

    public void setAvgBytes(FloatWritable avgBytes) {
        this.avgBytes = avgBytes;
    }

    public LongWritable getBytes() {
        return bytes;
    }

    public void setBytes(LongWritable bytes) {
        this.bytes = bytes;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        avgBytes.write(dataOutput);
        bytes.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        avgBytes.readFields(dataInput);
        bytes.readFields(dataInput);
    }

    @Override
    public String toString() {
        return avgBytes + "," + bytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IpWritable that = (IpWritable) o;
        return Objects.equal(avgBytes, that.avgBytes) &&
            Objects.equal(bytes, that.bytes);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(avgBytes, bytes);
    }

    @Override
    public int compareTo(IpWritable o) {
        int result = avgBytes.compareTo(o.avgBytes);
        if (0 == result) {
            result = bytes.compareTo(o.bytes);
        }
        return result;
    }
}
