package sparkcl;
//import com.amd.aparapi.internal.opencl.OpenCLLoader;
import com.aparapi.Kernel;
import com.aparapi.internal.opencl.OpenCLLoader;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import scala.Serializable;
import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
//import com.amd.aparapi.Range;
import com.aparapi.Range;
import org.apache.spark.sql.streaming.Trigger;



import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;
public final class SparkCLWordCount
{
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {

//        if (args.length < 1) {
//            System.err.println("Usage: WordCountCL <file>");
//            System.exit(1);
//        }
//
//        // get number of slices if available
//        int slices = (args.length >= 2) ? Integer.parseInt(args[1]) : 2;
//        System.out.printf("WordCountCL running on: %s (num of slices=%d)\n",args[0],slices);

        SparkConf sparkConf = new SparkConf();

        // if launched without config settings set default config values
        if(!sparkConf.contains("spark.master"))
        {
            sparkConf.setAppName("WordCountCL")
                    .setMaster("local[2]")
                    .set("spark.default.parallelism","2")
                    .set("spark.sql.shuffle.partitions","2")
                    .set("spark.executor.memory","1g");

        }


//        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
//
//
//
//        JavaRDD<String> lines = ctx.textFile(args[0], slices);
//
//
//        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public Iterator<String> call(String s) {
//                return Arrays.asList(SPACE.split(s)).iterator();
//            }
//        });
//
//        JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
//            @Override
//            public Tuple2<String, Integer> call(String s) {
//                return new Tuple2<String, Integer>(s, 1);
//            }
//        });
//
//        //
//        // Define a somewhat more complex kernel then other demos, includes:
//        // OpenCL work groups, local memory and barriers.
//        // Conditional execution
//        // Device select
//
//        SparkKernel<Tuple2<String, Iterable<Integer>>, Tuple2<String, Integer>> kernel = new SparkKernel<Tuple2<String, Iterable<Integer>>, Tuple2<String, Integer>>()
//        {
//            // data
//            int []dataArray;
//            int []sumArray;
//
//            // minimum amount of data before using accelerator
//            // note this should be significantly large, but kept small for the purpose of the demo
//            final int MinDataSizeForAcceleration = 10;
//
//            @Override
//            public void mapParameters(Tuple2<String, Iterable<Integer>> data)
//            {
//                dataArray = SparkUtil.intArrayFromIterator(data._2.iterator());
//                // decide if to execute the kernel or not
//                ///////////////////////////////////////////////
//                if(dataArray.length<MinDataSizeForAcceleration)
//                {
//                    setShouldExecute(false);
//                    return;
//                }
//                else
//                    setShouldExecute(true);
//                //////////////////////////////////////////////
//                // !!! temp hack -> handle a case where size is not divisible by two. Needs more work...
//                //////////////////////////////////////////////
//                if(dataArray.length%2!=0)
//                {
//                    int []tempArray = dataArray.clone();
//                    dataArray = new int[dataArray.length+1];
//                    for(int i=0;i<tempArray.length; i++)
//                        dataArray[i] = tempArray[i];
//                }
//                int dataLength = dataArray.length;
//                sumArray = new int[dataLength];
//                setRange(Range.create(dataLength));
//                setExecutionMode(EXECUTION_MODE.GPU);
//                buffer_$local$ = new int[getRange().getLocalSize(0)];
//            }
//
//            //@Local symbol does not seem to be working yet in aparapi
//            // we use $local$ convention instead
//            // define local memory type to improve performance. For more info on local memory ->
//            // https://www.khronos.org/registry/cl/sdk/1.1/docs/man/xhtml/local.html
//            int[] buffer_$local$;
//
//            @Override
//            public void run()
//            {
//
//                int gid = getGlobalId();
//                int lid = getLocalId();
//                int localSize = getLocalSize();
//                int localGroupIndex = gid / localSize;
//
//                final int upperGlobalIndexBound = getGlobalSize() - 1;
//                final int maxValidLocalIndex=localSize>>1;
//
//                int baseGlobalIndex = 2 * localSize * localGroupIndex + lid;
//
//                if(baseGlobalIndex<upperGlobalIndexBound)
//                    buffer_$local$[lid] = dataArray[baseGlobalIndex] + dataArray[baseGlobalIndex + 1];
//
//                localBarrier();
//
//                if(lid==0)
//                {
//                    for(int i=0;i<maxValidLocalIndex;i++)
//                        sumArray[localGroupIndex] += buffer_$local$[i];
//                }
//            }
//
//            @Override
//            public Tuple2<String, Integer> mapReturnValue(Tuple2<String, Iterable<Integer>> data)
//            {
//                int sum = 0;
//                // if kernel was executed
//                if(shouldExecute())
//                {
//                    for(int i=0;i<dataArray.length/getRange().getLocalSize(0);i++)
//                        sum += sumArray[i];
//                }
//                // kernel was not executed, not enough data, so perform a CPU simple aggregation
//                else
//                {
//                    Iterator<Integer> itr = data._2.iterator();
//                    while(itr.hasNext())
//                        sum+=itr.next();
//                }
//
//                return  new Tuple2<String, Integer>(data._1,sum);
//            }
//        };
//
//        // first group by key to try to aggregate enough data for our kernel to be able to work on efficiently
//        JavaRDD<Tuple2<String, Integer>> countTuples = SparkUtil.genSparkCL(ones.groupByKey()).mapCL(kernel );
//
//        // display word count results
//        List<Tuple2<String, Integer>> output = countTuples.collect();
//        for (Tuple2<?,?> tuple : output) {
//            System.out.println(tuple._1() + ": " + tuple._2());
//        }
//
//        ctx.stop();

        //streaming中wordcount的逻辑
        SparkSession spark = SparkSession
                .builder()
                .config(sparkConf)
                .appName("JavaStructuredNetworkWordCount")
                .getOrCreate();
        // Create DataFrame representing the stream of input lines from connection to host:port
        Dataset<Row> linesStream = spark
                .readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", "9999")
                .load();

        // Split the lines into words
        Dataset<String> wordsStream = linesStream.as(Encoders.STRING()).flatMap(
                (FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(),
                Encoders.STRING());

        // Generate running word count
//        Dataset<Row> wordCounts = wordsStream.groupBy("value").count();

//        final int size = 10240;
//
//        final float[] a = new float[size];
//        final float[] b = new float[size];
//
//        for (int i = 0; i < size; i++) {
//            a[i] = (float) (Math.random() * 100);
//            b[i] = (float) (Math.random() * 100);
//        }
//
//        final float[] sum = new float[size];

        //重写核函数
        SparkKernel<Dataset<String>,Dataset<Row>> kernel2 = new SparkKernel<Dataset<String>, Dataset<Row>>()
        {
            // data
            Dataset<Row> result;
            Dataset<String> input;
            @Override
            public void mapParameters(Dataset<String> data) {
                input = data;
                result = null;
                setRange(Range.create(1024));
//                setExecutionMode(EXECUTION_MODE.GPU);
                setExecutionModeWithoutFallback(EXECUTION_MODE.GPU);
            }

            @Override
            public void run() {
                result = input.groupBy("value").count();
//                int gid = getGlobalId();
//                sum[gid] = a[gid] + b[gid];
            }

            @Override
            public Dataset<Row> mapReturnValue(Dataset<String> data) {
//                Dataset<Row> returnValue = result;
                return input.groupBy("value").count();
//                return returnValue;
            }

        };
//        for (int i = 0; i < size; i++) {
//            System.out.printf("%6.2f + %6.2f = %8.2f\n", a[i], b[i], sum[i]);
//        }
        Dataset<Row> wordCounts = SparkUtil.genSparkCL3(wordsStream).mapCL3(kernel2);
        System.out.println(OpenCLLoader.isOpenCLAvailable());

        // Start running the query that prints the running counts to the console
        StreamingQuery query = wordCounts.writeStream()
                .outputMode("update")
                .format("console")
                .option("truncate", false)
                .trigger(Trigger.ProcessingTime("2 seconds"))
                .start();



        /*
            size为1024时看不到GPU被调度，不知道原因是资源使用太少没显示还是没来得及调用GPU
            size为10240可看到GPU占用0.2%左右
         */

//
//        final int size = 10240;
//
//        final float[] a = new float[size];
//        final float[] b = new float[size];
//
//        for (int i = 0; i < size; i++) {
//            a[i] = (float) (Math.random() * 100);
//            b[i] = (float) (Math.random() * 100);
//        }
//
//        final float[] sum = new float[size];
//
//        Kernel kernel = new Kernel(){
//            @Override public void run() {
//                int gid = getGlobalId();
//                sum[gid] = a[gid] + b[gid];
//            }
//        };
////      kernel.setExecutionMode(Kernel.EXECUTION_MODE.GPU);
//        kernel.setExecutionModeWithoutFallback(Kernel.EXECUTION_MODE.GPU);
//        kernel.execute(Range.create(size));
//
//        for (int i = 0; i < size; i++) {
//            System.out.printf("%6.2f + %6.2f = %8.2f\n", a[i], b[i], sum[i]);
//        }
//        System.out.println(kernel.getExecutionMode());
//
//        kernel.dispose();

        query.awaitTermination();
    }

}
