package sparkcl;

import java.io.Serializable;

public abstract class SparkKernel2<T> extends SparkKernelBase implements Serializable
{

    public abstract void mapParameters(T data1, T data2);

    public abstract void run();

    public abstract T mapReturnValue(T data1, T data2);

}
