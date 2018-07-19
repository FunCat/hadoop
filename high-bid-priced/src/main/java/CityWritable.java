import com.google.common.base.Objects;
import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CityWritable implements Writable {
    private IntWritable cityId;
    private IntWritable biddingPrice;
    private Text os;

    public CityWritable() {
        this.cityId = new IntWritable(0);
        this.biddingPrice = new IntWritable(0);
        this.os = new Text();
    }

    public CityWritable(IntWritable cityId, IntWritable biddingPrice, Text os) {
        this.cityId = cityId;
        this.biddingPrice = biddingPrice;
        this.os = os;
    }

    public void getCity(int cityId, int biddingPrice, String os) {
        this.cityId.set(cityId);
        this.biddingPrice.set(biddingPrice);
        this.os.set(os);
    }

    public IntWritable getCityId() {
        return cityId;
    }

    public void setCityId(IntWritable cityId) {
        this.cityId = cityId;
    }

    public IntWritable getBiddingPrice() {
        return biddingPrice;
    }

    public void setBiddingPrice(IntWritable biddingPrice) {
        this.biddingPrice = biddingPrice;
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
        biddingPrice.write(dataOutput);
        os.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        cityId.readFields(dataInput);
        biddingPrice.readFields(dataInput);
        os.readFields(dataInput);
    }

    @Override
    public String toString() {
        return biddingPrice.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CityWritable that = (CityWritable) o;
        return Objects.equal(cityId, that.cityId) &&
            Objects.equal(biddingPrice, that.biddingPrice) &&
            Objects.equal(os, that.os);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(cityId, biddingPrice, os);
    }
}
