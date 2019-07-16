package sparkcl;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Serializable;
import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SQLContext;
import com.amd.aparapi.Range;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;
public final class SparkCLWordCount
{
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: WordCountCL <file>");
            System.exit(1);
        }

        // get number of slices if available
        int slices = (args.length >= 2) ? Integer.parseInt(args[1]) : 2;
        System.out.printf("WordCountCL running on: %s (num of slices=%d)\n",args[0],slices);

//
//        SparkConf sparkConf = new SparkConf().setAppName("JavaSparkSQL");
//        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
//        SQLContext sqlCtx = new SQLContext(ctx);
//
//        // Load a text file and convert each line to a Java Bean.
//        JavaRDD<Person> people = ctx.textFile("examples/src/main/resources/people.txt").map(
//                new Function<String, Person>() {
//                    public Person call(String line) throws Exception {
//                        String[] parts = line.split(",");
//
//                        Person person = new Person();
//                        person.setName(parts[0]);
//                        person.setAge(Integer.parseInt(parts[1].trim()));
//
//                        return person;
//                    }
//                });
//
//        // Apply a schema to an RDD of Java Beans and register it as a table.
//
//        JavaSchemaRDD schemaPeople = sqlCtx.applySchema(people, Person.class);
//        schemaPeople.registerAsTable("people");
//
//        // SQL can be run over RDDs that have been registered as tables.
//        JavaSchemaRDD teenagers = sqlCtx.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19");
//
//        // The results of SQL queries are SchemaRDDs and support all the normal RDD operations.
//        // The columns of a row in the result can be accessed by ordinal.
//        List<String> teenagerNames = teenagers.map(new Function<Row, String>() {
//            public String call(Row row) {
//                return "Name: " + row.getString(0);
//            }
//        }).collect();


        SparkConf sparkConf = new SparkConf();

        // if launched without config settings set default config values
        if(!sparkConf.contains("spark.master"))
        {
            sparkConf.setAppName("WordCountCL")
                    .setMaster("local[2]")
                    .set("spark.executor.memory","1g");
        }

        JavaSparkContext ctx = new JavaSparkContext(sparkConf);



        JavaRDD<String> lines = ctx.textFile(args[0], slices);


        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) {
                return Arrays.asList(SPACE.split(s)).iterator();
            }
        });

        JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        //
        // Define a somewhat more complex kernel then other demos, includes:
        // OpenCL work groups, local memory and barriers.
        // Conditional execution
        // Device select

        SparkKernel<Tuple2<String, Iterable<Integer>>, Tuple2<String, Integer>> kernel = new SparkKernel<Tuple2<String, Iterable<Integer>>, Tuple2<String, Integer>>()
        {
            // data
            int []dataArray;
            int []sumArray;

            // minimum amount of data before using accelerator
            // note this should be significantly large, but kept small for the purpose of the demo
            final int MinDataSizeForAcceleration = 10;

            @Override
            public void mapParameters(Tuple2<String, Iterable<Integer>> data)
            {
                dataArray = SparkUtil.intArrayFromIterator(data._2.iterator());
                // decide if to execute the kernel or not
                ///////////////////////////////////////////////
                if(dataArray.length<MinDataSizeForAcceleration)
                {
                    setShouldExecute(false);
                    return;
                }
                else
                    setShouldExecute(true);
                //////////////////////////////////////////////
                // !!! temp hack -> handle a case where size is not divisible by two. Needs more work...
                //////////////////////////////////////////////
                if(dataArray.length%2!=0)
                {
                    int []tempArray = dataArray.clone();
                    dataArray = new int[dataArray.length+1];
                    for(int i=0;i<tempArray.length; i++)
                        dataArray[i] = tempArray[i];
                }
                int dataLength = dataArray.length;
                sumArray = new int[dataLength];
                setRange(Range.create(dataLength));
                setExecutionMode(EXECUTION_MODE.GPU);
                buffer_$local$ = new int[getRange().getLocalSize(0)];
            }

            //@Local symbol does not seem to be working yet in aparapi
            // we use $local$ convention instead
            // define local memory type to improve performance. For more info on local memory ->
            // https://www.khronos.org/registry/cl/sdk/1.1/docs/man/xhtml/local.html
            int[] buffer_$local$;

            @Override
            public void run()
            {

                int gid = getGlobalId();
                int lid = getLocalId();
                int localSize = getLocalSize();
                int localGroupIndex = gid / localSize;

                final int upperGlobalIndexBound = getGlobalSize() - 1;
                final int maxValidLocalIndex=localSize>>1;

                int baseGlobalIndex = 2 * localSize * localGroupIndex + lid;

                if(baseGlobalIndex<upperGlobalIndexBound)
                    buffer_$local$[lid] = dataArray[baseGlobalIndex] + dataArray[baseGlobalIndex + 1];

                localBarrier();

                if(lid==0)
                {
                    for(int i=0;i<maxValidLocalIndex;i++)
                        sumArray[localGroupIndex] += buffer_$local$[i];
                }
            }

            @Override
            public Tuple2<String, Integer> mapReturnValue(Tuple2<String, Iterable<Integer>> data)
            {
                int sum = 0;
                // if kernel was executed
                if(shouldExecute())
                {
                    for(int i=0;i<dataArray.length/getRange().getLocalSize(0);i++)
                        sum += sumArray[i];
                }
                // kernel was not executed, not enough data, so perform a CPU simple aggregation
                else
                {
                    Iterator<Integer> itr = data._2.iterator();
                    while(itr.hasNext())
                        sum+=itr.next();
                }

                return  new Tuple2<String, Integer>(data._1,sum);
            }
        };

        // first group by key to try to aggregate enough data for our kernel to be able to work on efficiently
        JavaRDD<Tuple2<String, Integer>> countTuples = SparkUtil.genSparkCL(ones.groupByKey()).mapCL(kernel );

        // display word count results
        List<Tuple2<String, Integer>> output = countTuples.collect();
        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }

        ctx.stop();
    }

    public static class Person implements Serializable {
        private String name;
        private int age;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }
}
