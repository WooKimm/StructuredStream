package sparkcl;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.Iterator;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import com.amd.aparapi.Config;
//import com.aparapi.Config;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.types.StructType;
import scala.reflect.ClassTag;


public class SparkCL<T> implements Serializable
{
    /**
     *  Serialization version number
     */

    private static final long serialVersionUID = -5077883727987463648L;

    // lock base name, changes according to platform/device
    private static final String KERNEL_PROCESS_SYNC_FILE = "Kernel.Process.Sync.file";

    // kernel lock timeout
    // TODO: revise lock time, deadlock starvation issues.
    final long KernelTimeOutMill = 60 * 1000;

    // data
    JavaRDDLike<T,?> m_data;
    Dataset<T> d_data;
    T ds_data;

    public SparkCL(JavaRDDLike<T, ?> data)
    {
        m_data = data;
    }
    public SparkCL(Dataset<T> data){d_data = data;}
    public SparkCL(T data){ds_data=data;}

    private void exclusiveExecKernel(SparkKernelBase kernel) throws Exception
    {
        // give user control over execution
        if(!kernel.shouldExecute())
            return;
        // lock name depends on com.amd.aparapi.Config.platformHint - per platform lock
        String lockName = KERNEL_PROCESS_SYNC_FILE + "." + Config.platformHint;
        // try to gain exclusive access to platform -
        // Several platform fail/misbehave when you try to share compute resources (NVidia GPUs/Altera FPGAs etc.)
        // for now we continue after KernelTimeOutMill and let the hardware delegate the resource failures/exceptions later on when we try to share the resources
        try (ProcessSync.ExclusiveAccess exls= new ProcessSync.ExclusiveAccess(lockName,KernelTimeOutMill);)
        {
            kernel.execute(kernel.getRange());
        }

        // Spark does not seem to free resources properly so explicitly call close
        // If user uses try-with-resources then we do not need to do that, but we don't won't to burden the user for now.
        if(kernel.shouldAutoReleaseResources())
            kernel.close();

    }

    public <R> JavaRDD<R> mapCL(final SparkKernel<T,R> kernel)
    {

        Function<T,R> funcX = new Function<T,R>()
        {

            @Override
            public R call(T v1) throws Exception
            {
                final SparkKernel<T,R> cachedKernel = (SparkKernel<T, R>) SparkCLCache.getInstance().tryGetCachedKernelVersion(kernel);
                cachedKernel.mapParameters(v1);
                exclusiveExecKernel(cachedKernel);
                return cachedKernel.mapReturnValue(v1);
            }

        };

        return (JavaRDD<R>) m_data.map(funcX);
    }

    public <R> JavaRDD<R> mapCLPartition(final SparkKernel<java.util.Iterator<T>,java.util.Iterator<R>> kernel)
    {

        Function2 mapRDDFunc= new Function2<Integer, java.util.Iterator<T>, java.util.Iterator<R>>()
        {
            @Override
            public Iterator<R> call(Integer v1, Iterator<T> v2)	throws Exception
            {
                final SparkKernel<java.util.Iterator<T>,java.util.Iterator<R>> cachedKernel = (SparkKernel<java.util.Iterator<T>,java.util.Iterator<R>>) SparkCLCache.getInstance().tryGetCachedKernelVersion(kernel);
                cachedKernel.mapParameters(v2);
                exclusiveExecKernel(cachedKernel);
                return cachedKernel.mapReturnValue(v2);
            }
        };

        return m_data.mapPartitionsWithIndex(mapRDDFunc,true);
    }

//	public <R> JavaRDD<R> mapCLPartition(final SparkKernel<java.util.Iterator<T>,java.util.Iterator<R>> kernel)
//	{
//		return mapCLPartition(kernel,true);
//	}

    public T reduceCL(final SparkKernel2<T> kernel)
    {
        Function2<T,T,T> funcX = new Function2<T,T,T>()
        {
            @Override
            public T call(T v1, T v2) throws Exception
            {
                ///////////////////////////////////////////
                // debug print where reduce is performed...
                ///////////////////////////////////////////
                String computername=InetAddress.getLocalHost().getHostName();
                System.out.printf("--------\nWorking on %s: \n--------\n",computername);
                /////////////////////////////////////

                final SparkKernel2<T> cachedKernel = (SparkKernel2<T>) SparkCLCache.getInstance().tryGetCachedKernelVersion(kernel);
                cachedKernel.mapParameters(v1,v2);
                exclusiveExecKernel(cachedKernel);
                return cachedKernel.mapReturnValue(v1,v2);

            }
        };

        ///////////////////////
        // Note:
        ///////////////////////
        // current version of Spark reduce seems to be working on the driver:
        // http://mail-archives.apache.org/mod_mbox/incubator-spark-commits/201407.mbox/%3Cecd0ada13ba546578c6cc14cf8e9e1c7@git.apache.org%3E
        // we want to execute on the accelerated workers as much as possible!
        // Tree reduce seems to do this job for us by executing all but last level on the workers.
        //return m_data.reduce(funcX);
        return m_data.treeReduce(funcX,2);
    }

    //todo: 尝试重载/重写以上方法，使得支持dataset
    //传进带有T的kernel，返回Dataset<R>
    public <R> Dataset<R> mapCL2(final SparkKernel<T,R> kernel)
    {
        MapFunction<T,R> funcX = new MapFunction<T,R>()
        {

            @Override
            public R call(T v1) throws Exception
            {
                final SparkKernel<T,R> cachedKernel = (SparkKernel<T, R>) SparkCLCache.getInstance().tryGetCachedKernelVersion(kernel);
                cachedKernel.mapParameters(v1);
                exclusiveExecKernel(cachedKernel);
                //传出R类型
                return cachedKernel.mapReturnValue(v1);
            }

        };

        return  d_data.map(funcX, new Encoder<R>() {
            @Override
            public StructType schema() {
                //todo: 根据数据来进行指定
                return null;
            }

            @Override
            public ClassTag<R> clsTag() {
                return null;
            }
        });
    }

    //传进来的kernel中T相当于Dataset，返回出去的R也相当于Dataset
    public <R> R mapCL3(final SparkKernel<T,R> kernel)
    {
        MapFunction<T,R> funcX = new MapFunction<T,R>()
        {

            @Override
            public R call(T v1) throws Exception
            {
                final SparkKernel<T,R> cachedKernel = (SparkKernel<T, R>) SparkCLCache.getInstance().tryGetCachedKernelVersion(kernel);
                cachedKernel.mapParameters(v1);
                exclusiveExecKernel(cachedKernel);
                //传出R类型
                return cachedKernel.mapReturnValue(v1);
            }

        };

        try {
            return  funcX.call(ds_data);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }



}
