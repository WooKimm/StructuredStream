package sparkcl;

public class SparkCLCache
{
    private static SparkCLCache m_instance = null;

    protected SparkCLCache()
    {
        // Exists only to defeat instantiation.
    }

    public static SparkCLCache getInstance()
    {
        if(m_instance == null) {
            m_instance = new SparkCLCache();
        }
        return m_instance;
    }

    //
    // kernel cache - improve performance by reducing kernel init overhead
    //
    SparkKernelBase tryGetCachedKernelVersion(SparkKernelBase kernel)
    {
        if(m_cachedKernel==null)
            m_cachedKernel=kernel;

        return m_cachedKernel;
    }

    // data
    SparkKernelBase m_cachedKernel;
}

