package sparkcl;

import java.io.Serializable;

public abstract class SparkKernel<T,R> extends SparkKernelBase implements Serializable
{

    public abstract void mapParameters(T data);

    public abstract void run();

    public abstract R mapReturnValue(T data);

}
