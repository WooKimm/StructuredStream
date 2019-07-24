package sparkcl;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;



//import com.amd.aparapi.Range;
import com.aparapi.Range;
import org.apache.spark.sql.Dataset;

import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;

/**
 * Computes an approximation to pi using several methods: SparkCL, Spark original, Spark Optimized  etc..
 * Usage: SparkCLPi [slices] [slice size] [exec method name]
 */
public final class PiTest
{
    // define some constants
    static String SparkName   = "Spark";
    static String SparkCLName = "SparkCL";
    static String AppNameBase = "SparkCLPi";

    public static void main(String[] args) throws Exception {

        SparkConf sparkConf = new SparkConf();

        // if launched without config settings set default config values
        if(!sparkConf.contains("spark.master"))
        {
            sparkConf//.setAppName("SparkCLPi")
                    .setMaster("local[2]")
                    .set("spark.executor.memory","1g");
        }

        int slices = (args.length >= 1) ? Integer.parseInt(args[0]) : 2;

        int sliceSize = (args.length >= 2) ? Integer.parseInt(args[1]) : 100000;

        // read execution method from cmd line
        String methodName = (args.length >= 3) ? args[2] : "runSparkCLMapVersion";

        // make app name more meaningful for our tests
        String fqnAppName = sparkConf.get("spark.app.name", AppNameBase);
        fqnAppName +=  String.format(" - args(%d,%d,%s)",slices,sliceSize,methodName);
        sparkConf.setAppName(fqnAppName);

        // dynamically invoke test method
        java.lang.reflect.Method method = SparkUtil.getMethodByName(PiTest.class.getName(),methodName,new Class[] {JavaSparkContext.class, Integer.TYPE, Integer.TYPE});
        if(method==null)
        {
            System.err.println("Bad method name: try another function name");
            throw new Exception("Bad method name");
        }

        System.out.println("Calculating Pi using -> " + fqnAppName );

        // real code starts below ...
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        int count = (int) method.invoke(null,jsc, slices, sliceSize);

        System.out.println("Pi is roughly " + 4.0 * count / (sliceSize * slices) );

        jsc.stop();
    }

    static int runSparkOriginalFunc(JavaSparkContext jsc, int slices, int sliceSize)
    {
        int n = sliceSize * slices;

        List<Integer> l = new ArrayList<Integer>(n);
        for (int i = 0; i < n; i++) {
            l.add(i);
        }

        JavaRDD<Integer> dataSet = jsc.parallelize(l, slices);

        int count = dataSet.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) {
                double x = Math.random() * 2 - 1;
                double y = Math.random() * 2 - 1;
                return (x * x + y * y < 1) ? 1 : 0;
            }
        }).reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) {
                return integer + integer2;
            }
        });

        return count;
    }


    static int runSparkMapOptimizedVersion(JavaSparkContext jsc, int slices, int sliceSize)
    {
        // we don't need to allocate this huge array, we just want to send the amount of calculations on each node
        List<Integer> l = new ArrayList<Integer>(slices);
        for (int i = 0; i < slices; i++) {
            l.add(sliceSize);
        }

        JavaRDD<Integer> dataSet = jsc.parallelize(l, slices);

        int count = dataSet.map(new Function<Integer, Integer>() {


            @Override
            public Integer call(Integer integer) {
                return genRandomNumbers(integer);
            }

            // try to make a tight loop to improve CPU efficiency
            private Integer genRandomNumbers(int count) {
                int sum = 0;
                for (int i = 0; i < count; i++)	{
                    double x = Math.random() * 2 - 1;
                    double y = Math.random() * 2 - 1;
                    sum += (x * x + y * y < 1) ? 1 : 0;
                }
                return sum;
            }
        }).reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) {
                return integer + integer2;
            }
        });

        return count;
    }


    static int runSparkCLMapPartitionVersion(JavaSparkContext jsc, int slices, int sliceSize)
    {
        int n = sliceSize * slices;

        List<Integer> l = new ArrayList<Integer>(n);
        for (int i = 0; i < n; i++) {
            l.add(i);
        }

        JavaRDD<Integer> dataSet = jsc.parallelize(l, slices);

        SparkKernel<java.util.Iterator<Integer>,java.util.Iterator<Integer>> kernel = new SparkKernel<java.util.Iterator<Integer>,java.util.Iterator<Integer>>()
        {
            // data
            int[] sum;
            double[] randNumArray;

            @Override
            public void mapParameters(Iterator<Integer> data) {
                sum = new int[SparkUtil.intArrayFromIterator(data).length];
                genRandomNumbers(sum.length * 2);
                setRange(Range.create(sum.length));
                // setExecutionMode(EXECUTION_MODE.JTP); // choose run mode i.e run on JTP/CPU/Accelerator etc.
            }

            void genRandomNumbers(int count) {
                randNumArray = new double[count];
                for (int i = 0; i < count; i++) {
                    randNumArray[i] = Math.random();
                }
            }

            @Override
            public void run() {
                int gid = getGlobalId();
                int baseIndex = gid * 2;
                double x = randNumArray[baseIndex] * 2 - 1;
                double y = randNumArray[baseIndex + 1] * 2 - 1;
                sum[gid] = (x * x + y * y < 1) ? 1 : 0;
            }

            @Override
            public Iterator<Integer> mapReturnValue(Iterator<Integer> data) {
                // TODO Auto-generated method stub
                return SparkUtil.intArrayToIterator(sum);
            }

        };

        JavaRDD<Integer> dataSet2 = SparkUtil.genSparkCL(dataSet).mapCLPartition(kernel);

        int count = dataSet2.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) {
                return integer + integer2;
            }
        });

        return count;
    }

    static int runSparkCLMapVersion(JavaSparkContext jsc, int slices, final int sliceSize)
    {

        // we don't need to allocate this huge array, we just want to send the amount of calculations on each node
        List<Integer> l = new ArrayList<Integer>(slices);
        for (int i = 0; i < slices; i++) {
            l.add(sliceSize);
        }

        JavaRDD<Integer> dataSet = jsc.parallelize(l, slices);

        SparkKernel<Integer,Integer> kernel = new SparkKernel<Integer,Integer>()
        {
            // data
            int[] sum;
            double[] randNumArray;

            void genRandomNumbers(int count) {
                if(randNumArray==null)
                    randNumArray = new double[count];
                for (int i = 0; i < count; i++) {
                    randNumArray[i] = Math.random();
                }
            }

            @Override
            public void mapParameters(Integer data) {
                if(sum==null)
                    sum = new int[data];
                genRandomNumbers(sum.length * 2);
                setRange(Range.create(sum.length));
//                setExecutionMode(EXECUTION_MODE.JTP);
                setExecutionModeWithoutFallback(EXECUTION_MODE.GPU);
                // setExecutionMode(EXECUTION_MODE.JTP); // choose run mode i.e run on JTP/CPU/Accelerator etc.
            }

            @Override
            public void run() {
                int gid = getGlobalId();
                int baseIndex = gid * 2;
                double x = randNumArray[baseIndex] * 2 - 1;
                double y = randNumArray[baseIndex + 1] * 2 - 1;
                sum[gid] = (x * x + y * y < 1) ? 1 : 0;
            }

            @Override
            public Integer mapReturnValue(Integer data) {
                int totalSum = 0;
                for(int i=0;i<sum.length;i++)
                    totalSum += sum[i];
                return totalSum;
            }

        };

        JavaRDD<Integer> dataSet2 = SparkUtil.genSparkCL(dataSet).mapCL(kernel);

        int count = dataSet2.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) {
                return integer + integer2;
            }
        });

        return count;
    }

    // c++ style template support for primitive types would come handy right about now...
    // java does not support primitive types in generics hence the ugly code duplication
    static int runSparkCLMapVersionFloat(JavaSparkContext jsc, int slices, int sliceSize)
    {

        // we don't need to allocate this huge array, we just want to send the amount of calculations on each node
        List<Integer> l = new ArrayList<Integer>(slices);
        for (int i = 0; i < slices; i++) {
            l.add(sliceSize);
        }

        JavaRDD<Integer> dataSet = jsc.parallelize(l, slices);

        SparkKernel<Integer,Integer> kernel = new SparkKernel<Integer,Integer>()
        {
            // data
            int[] sum;
            float[] randNumArray;

            void genRandomNumbers(int count) {
                randNumArray = new float[count];
                for (int i = 0; i < count; i++) {
                    randNumArray[i] = (float)Math.random();
                }
            }

            @Override
            public void mapParameters(Integer data) {
                sum = new int[data];
                genRandomNumbers(sum.length * 2);
                setRange(Range.create(sum.length));
                // this.setExecutionMode(EXECUTION_MODE.JTP);
            }

            @Override
            public void run() {
                int gid = getGlobalId();
                int baseIndex = gid * 2;
                float x = randNumArray[baseIndex] * 2 - 1;
                float y = randNumArray[baseIndex + 1] * 2 - 1;
                sum[gid] = (x * x + y * y < 1) ? 1 : 0;
            }

            @Override
            public Integer mapReturnValue(Integer data) {
                int totalSum = 0;
                for(int i=0;i<sum.length;i++)
                    totalSum += sum[i];
                return totalSum;
            }

        };

        JavaRDD<Integer> dataSet2 = SparkUtil.genSparkCL(dataSet).mapCL(kernel);

        int count = dataSet2.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) {
                return integer + integer2;
            }
        });

        return count;
    }

}
