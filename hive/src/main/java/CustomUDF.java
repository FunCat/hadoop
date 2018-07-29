import eu.bitwalker.useragentutils.Browser;
import eu.bitwalker.useragentutils.OperatingSystem;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;

public class CustomUDF extends GenericUDF {

    private Object[] result;

    @Override
    public String getDisplayString(String[] arg0) {
        return "My display string";
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arg0) throws UDFArgumentException {
        // Define the field names for the struct<> and their types
        ArrayList<String> structFieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> structFieldObjectInspectors = new ArrayList<ObjectInspector>();

        // fill struct field names
        // type
        structFieldNames.add("type");
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        //family
        structFieldNames.add("family");
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        // OS name
        structFieldNames.add("os");
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        // device
        structFieldNames.add("device");
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);

        StructObjectInspector si = ObjectInspectorFactory.getStandardStructObjectInspector(structFieldNames,
            structFieldObjectInspectors);
        return si;
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        if (args == null || args.length < 1) {
            throw new HiveException("args is empty");
        }
        if (args[0].get() == null) {
            throw new HiveException("args contains null instead of object");
        }

        Object argObj = args[0].get();

        String argument = null;
        if (argObj instanceof Text){
            argument = argObj.toString();
        } else if (argObj instanceof String) {
            argument = (String) argObj;
        } else if(argObj instanceof LazyString) {
            argument = argObj.toString();
        } else {
            throw new HiveException("Argument is neither a Text nor String, it is a " + argObj.getClass().getCanonicalName());
        }

        return parseUAString(argument);
    }

    private Object parseUAString(String argument) {
        result = new Object[4];
        UserAgent ua = new UserAgent(argument);
        result[0] = new Text(ua.getBrowser().getBrowserType().getName());
        result[1] = new Text(ua.getBrowser().getGroup().getName());
        result[2] = new Text(ua.getOperatingSystem().getName());
        result[3] = new Text(ua.getOperatingSystem().getDeviceType().getName());
        return result;
    }
}

