package sparkcl;

import com.aparapi.Range;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class VectorAdd {
    public static void main(String[] args) throws Exception
    {
        SparkConf sparkConf = new SparkConf();

        // if launched without config settings set default config values
        if(!sparkConf.contains("spark.master"))
        {
            sparkConf.setAppName("SparkCLVectorAdd")
                    .setMaster("local[2]")
                    .set("spark.executor.memory","1g");
        }

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        int slices = (args.length >= 1) ? Integer.parseInt(args[0]) : 96;

        int sliceSize = (args.length >= 2) ? Integer.parseInt(args[1]) : 100000;

        //testVectorAddUsingReduceCL(jsc,slices,sliceSize);
        testVectorAddUsingReduceCLWithMapFirst(jsc,slices,sliceSize);

        jsc.stop();

    }

    // demo accelerated vector edition using reduceCL
    private static void testVectorAddUsingReduceCL(JavaSparkContext jsc, int slices, int sliceSize)
    {
        //
        // create a custom class to hold vector data
        //
        class RddTestArray implements Serializable
        {
            private static final long serialVersionUID = -5723623480419092114L;

            public RddTestArray(float []dataArray)
            {
                m_dataArray = dataArray; // note we do not copy the array, we just point to it.
            }

            public float[] getDataArray()
            {
                return m_dataArray;
            }

            // data
            float []m_dataArray;
        }

        float[] data = new float[sliceSize];

        // initialize elements to 1, to make it easy to validate functionality
        Arrays.fill(data,1);

        List<RddTestArray> l = new ArrayList<RddTestArray>(slices);
        for (int i = 0; i < slices; i++)
        {
            l.add(new RddTestArray(data));
        }

        JavaRDD<RddTestArray> rddTest = jsc.parallelize(l,slices);

        //
        // ReduceCL
        //

        RddTestArray rddTestArrayRes = SparkUtil.genSparkCL(rddTest).reduceCL(new SparkKernel2<RddTestArray>()
        {
            // data
            float[] a;
            float[] b;
            float[] c;

            @Override
            public void mapParameters(RddTestArray data1, RddTestArray data2)
            {
                a = data1.getDataArray();
                b = data2.getDataArray();
                c = new float[a.length];
                setRange(Range.create(c.length));
                // optional -> choose kernel execution mode
                //setExecutionMode(EXECUTION_MODE.CPU);
            }

            @Override
            public void run()
            {
                int gid = getGlobalId();
                c[gid] = a[gid] + b[gid];
            }

            @Override
            public RddTestArray mapReturnValue(RddTestArray data1, RddTestArray data2)
            {
                return new RddTestArray(c);
            }

        });

        // reduce the final array -
        // note - we could do another type of map/reduce instead the final loop if the final array is to big
        float count = 0;
        for(int i=0; i<rddTestArrayRes.getDataArray().length;i++)
            count += rddTestArrayRes.getDataArray()[i];

        System.out.println("Sum is " + count );

    }

    // demo accelerated vector edition using reduceCL version with map before reduce
    private static void testVectorAddUsingReduceCLWithMapFirst(JavaSparkContext jsc, int slices, int sliceSize)
    {
        //
        // create a custom class to hold vector data
        //
        class RddTestArray implements Serializable
        {
            private static final long serialVersionUID = -5723623480419092114L;

            public RddTestArray(float []dataArray)
            {
                m_dataArray = dataArray; // note we do not copy the array, we just point to it.
            }

            public float[] getDataArray()
            {
                return m_dataArray;
            }

            // data
            float []m_dataArray;
        }

        List<Integer> l = new ArrayList<Integer>(slices);
        for (int i = 0; i < slices; i++)
        {
            l.add(sliceSize);
        }

        JavaRDD<Integer> rddTest = jsc.parallelize(l,slices);

        //
        // map size to array of data elements
        //
        Function<Integer, RddTestArray> mapFunc = new Function<Integer, RddTestArray>(){

            @Override
            public RddTestArray call(Integer sliceSize) throws Exception
            {
                float[] data = new float[sliceSize];

                // initialize elements to 1, to make it easy to validate functionality
                Arrays.fill(data,1);

                return new RddTestArray(data);
            }};

        JavaRDD<RddTestArray> rddTest1 = rddTest.map(mapFunc);

        //
        // ReduceCL
        //

        RddTestArray rddTestArrayRes = SparkUtil.genSparkCL(rddTest1).reduceCL(new SparkKernel2<RddTestArray>()
        {
            // data
            float[] a;
            float[] b;
            float[] c;

            @Override
            public void mapParameters(RddTestArray data1, RddTestArray data2)
            {
                a = data1.getDataArray();
                b = data2.getDataArray();
                c = new float[a.length];
                setRange(Range.create(c.length));
                setExecutionModeWithoutFallback(EXECUTION_MODE.GPU);
                //setExecutionMode(EXECUTION_MODE.CPU);
            }

            @Override
            public void run()
            {
                int gid = getGlobalId();
                c[gid] = a[gid] + b[gid];
            }

            @Override
            public RddTestArray mapReturnValue(RddTestArray data1, RddTestArray data2)
            {
                return new RddTestArray(c);
            }

        });

        // reduce the final array -
        // note - we could do another type of map/reduce instead the final loop if the final array is to big
        float count = 0;
        for(int i=0; i<rddTestArrayRes.getDataArray().length;i++)
            count += rddTestArrayRes.getDataArray()[i];

        System.out.println("Sum is " + count );

    }
}
