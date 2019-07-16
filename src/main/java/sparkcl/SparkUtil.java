package sparkcl;
import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.sql.Dataset;

import java.util.ArrayList;
import java.util.Iterator;

public class SparkUtil {
    public static <T> SparkCL<T> genSparkCL(JavaRDDLike<T,?> data)
    {
        return new SparkCL<T>(data);
    }
    public static <T> SparkCL<T> genSparkCL2(Dataset<T> data){
        return new SparkCL<T>(data);
    }
    public static <T> SparkCL<T> genSparkCL3(T data){
        return new SparkCL<T>(data);
    }

    public static <T> ArrayList<T> getArrayFromIterator(Iterator<T> dataListItr)
    {
        final ArrayList<T> dataList = new ArrayList<T>();

        while(dataListItr.hasNext())
        {
            dataList.add(dataListItr.next());
        }
        return dataList;
    }

    // Unfortunately generics in Java (Unlike C++ for example) do not allow to use simple types in templates:
    // https://www.java.net/node/643733
    // so below are some ugly special purpose functions...
    public static int[] intArrayFromIterator(Iterator<Integer> dataListItr)
    {
        ArrayList<Integer> tempArray= SparkUtil.getArrayFromIterator(dataListItr);
        Integer[] integerArray = new Integer[tempArray.size()];
        tempArray.toArray(integerArray);
        return ArrayUtils.toPrimitive(integerArray);
    }

	/*
	public static float[] floatArrayFromIterator(Iterator<Float> dataListItr)
	{
        //Float [] temp = (Float[]) SparkUtil.getArrayFromIterator(dataListItr);
        return ArrayUtils.toPrimitive(SparkUtil.getArrayFromIterator(dataListItr));
	}
    */

    public static Iterator<Integer> intArrayToIterator(final int [] primitiveArray)
    {
        java.lang.Iterable<Integer> aIterable=new Iterable<Integer>() {

            public Iterator<Integer> iterator() {
                return new Iterator<Integer>() {
                    private int pos=0;

                    public boolean hasNext() {
                        return primitiveArray.length>pos;
                    }

                    public Integer next() {
                        return primitiveArray[pos++];
                    }

                    public void remove() {
                        throw new UnsupportedOperationException("Cannot remove an element of an array.");
                    }
                };
            }
        };
        return aIterable.iterator();
    }


    public static java.lang.reflect.Method getMethodByName(String className, String methodName, Class[] argTypes)
    {
        java.lang.reflect.Method method = null;
        try {
            Class<?> c = Class.forName(className);
            method = c.getDeclaredMethod(methodName, argTypes);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return method;
    }
}
