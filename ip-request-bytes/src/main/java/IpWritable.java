import com.google.common.base.Objects;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * The IpWritable represents the information about the IP connection. It contains the name IP,
 * average count of bytes and total bytes.
 */
public class IpWritable implements Writable {
    private Text ip;
    private FloatWritable avgBytes;
    private LongWritable bytes;

    public IpWritable() {
        this.ip = new Text();
        this.avgBytes = new FloatWritable(0);
        this.bytes = new LongWritable(0);
    }

    public IpWritable(Text ip, FloatWritable avgBytes, LongWritable bytes) {
        this.ip = ip;
        this.avgBytes = avgBytes;
        this.bytes = bytes;
    }

    public void getIp(String ip, float avgBytes, String bytes) {
        this.ip.set(ip);
        this.avgBytes.set(avgBytes);
        this.bytes.set(Long.valueOf(bytes));
    }

    public Text getIp() {
        return ip;
    }

    public void setIp(Text ip) {
        this.ip = ip;
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
        ip.write(dataOutput);
        avgBytes.write(dataOutput);
        bytes.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        ip.readFields(dataInput);
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
        return Objects.equal(ip, that.ip) &&
            Objects.equal(avgBytes, that.avgBytes) &&
            Objects.equal(bytes, that.bytes);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(ip, avgBytes, bytes);
    }
}
