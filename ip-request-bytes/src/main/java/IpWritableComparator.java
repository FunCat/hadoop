import org.apache.hadoop.io.WritableComparator;

public class IpWritableComparator extends WritableComparator {

    protected IpWritableComparator() {
        super(IpWritable.class);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        byte[] result1 = new byte[l1];
        System.arraycopy(b1, s1, result1, 0, l1);
        byte[] result2 = new byte[l2];
        System.arraycopy(b2, s2, result2, 0, l2);

        String str1 = new String(result1);
        String str2 = new String(result2);

        return str1.compareTo(str2);
    }
}
