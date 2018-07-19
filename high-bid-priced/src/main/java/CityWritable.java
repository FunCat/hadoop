import com.google.common.base.Objects;
import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * The CityWritable represents the information about the city. It contains the city id,
 * amount of high-bid-priced which is more than 250 and the operation system.
 */
public class CityWritable implements Writable {
    private IntWritable cityId;
    private IntWritable counter;
    private Text os;

    public CityWritable() {
        this.cityId = new IntWritable(0);
        this.counter = new IntWritable(0);
        this.os = new Text();
    }

    public CityWritable(IntWritable cityId, IntWritable counter, Text os) {
        this.cityId = cityId;
        this.counter = counter;
        this.os = os;
    }

    public void getCity(int cityId, int counter, String os) {
        this.cityId.set(cityId);
        this.counter.set(counter);
        this.os.set(os);
    }

    public IntWritable getCityId() {
        return cityId;
    }

    public void setCityId(IntWritable cityId) {
        this.cityId = cityId;
    }

    public IntWritable getCounter() {
        return counter;
    }

    public void setCounter(IntWritable counter) {
        this.counter = counter;
    }

    public Text getOs() {
        return os;
    }

    public void setOs(Text os) {
        this.os = os;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        cityId.write(dataOutput);
        counter.write(dataOutput);
        os.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        cityId.readFields(dataInput);
        counter.readFields(dataInput);
        os.readFields(dataInput);
    }

    @Override
    public String toString() {
        return counter.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CityWritable that = (CityWritable) o;
        return Objects.equal(cityId, that.cityId) &&
            Objects.equal(counter, that.counter) &&
            Objects.equal(os, that.os);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(cityId, counter, os);
    }
}
