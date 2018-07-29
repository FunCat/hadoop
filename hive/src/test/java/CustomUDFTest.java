import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CustomUDFTest {

    @Test
    public void testUDF() throws HiveException {
        CustomUDF example = new CustomUDF();
        Text input = new Text("Mozilla/5.0 (compatible; MSIE 9.0;\\Windows NT 6.1; WOW64; Trident/5.0)");
        GenericUDF.DeferredObject[] test = new GenericUDF.DeferredObject[]{new GenericUDF.DeferredJavaObject(input)};
        Object[] result = new Object[] {new Text("Browser"), new Text("Internet Explorer"), new Text("Windows 7"), new Text("Computer")};

        Object[] evaluate = (Object[]) example.evaluate(test);
        assertEquals(result[0].toString(), evaluate[0].toString());
        assertEquals(result[1].toString(), evaluate[1].toString());
        assertEquals(result[2].toString(), evaluate[2].toString());
        assertEquals(result[3].toString(), evaluate[3].toString());
    }

    @Test
    public void testUDF2() throws HiveException {
        CustomUDF example = new CustomUDF();
        Text input = new Text("Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.2; SV1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)");
        GenericUDF.DeferredObject[] test = new GenericUDF.DeferredObject[]{new GenericUDF.DeferredJavaObject(input)};
        Object[] result = new Object[] {new Text("Browser"), new Text("Internet Explorer"), new Text("Windows XP"), new Text("Computer")};

        Object[] evaluate = (Object[]) example.evaluate(test);
        assertEquals(result[0].toString(), evaluate[0].toString());
        assertEquals(result[1].toString(), evaluate[1].toString());
        assertEquals(result[2].toString(), evaluate[2].toString());
        assertEquals(result[3].toString(), evaluate[3].toString());
    }

    @Test
    public void testUDF3() throws HiveException {
        CustomUDF example = new CustomUDF();
        Text input = new Text("Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.17 (KHTML, like Gecko)");
        GenericUDF.DeferredObject[] test = new GenericUDF.DeferredObject[]{new GenericUDF.DeferredJavaObject(input)};
        Object[] result = new Object[] {new Text("Browser"), new Text("Apple WebKit"), new Text("Windows XP"), new Text("Computer")};

        Object[] evaluate = (Object[]) example.evaluate(test);
        assertEquals(result[0].toString(), evaluate[0].toString());
        assertEquals(result[1].toString(), evaluate[1].toString());
        assertEquals(result[2].toString(), evaluate[2].toString());
        assertEquals(result[3].toString(), evaluate[3].toString());
    }

    @Test
    public void testUDF4() throws HiveException {
        CustomUDF example = new CustomUDF();
        Text input = new Text("Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)");
        GenericUDF.DeferredObject[] test = new GenericUDF.DeferredObject[]{new GenericUDF.DeferredJavaObject(input)};
        Object[] result = new Object[] {new Text("Browser"), new Text("Internet Explorer"), new Text("Windows XP"), new Text("Computer")};

        Object[] evaluate = (Object[]) example.evaluate(test);
        assertEquals(result[0].toString(), evaluate[0].toString());
        assertEquals(result[1].toString(), evaluate[1].toString());
        assertEquals(result[2].toString(), evaluate[2].toString());
        assertEquals(result[3].toString(), evaluate[3].toString());
    }
}