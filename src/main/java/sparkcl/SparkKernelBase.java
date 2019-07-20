package sparkcl;

//import com.amd.aparapi.Kernel;
//import com.amd.aparapi.Range;
//
import com.aparapi.Kernel;
import com.aparapi.Range;
public abstract class SparkKernelBase extends Kernel implements AutoCloseable
{
    // data
    private Range m_range;
    private boolean m_shouldExecute = true;
    private boolean m_autoReleaseResources = false;

    // handle on close
    @Override
    public void close() { dispose(); }

    protected boolean shouldExecute() {
        return m_shouldExecute;
    }

    protected void setShouldExecute(boolean m_shouldExecute) {
        this.m_shouldExecute = m_shouldExecute;
    }

    protected boolean shouldAutoReleaseResources() {
        return m_autoReleaseResources;
    }

    protected void setAutoReleaseResources(boolean m_autoReleaseResources) {
        this.m_autoReleaseResources = m_autoReleaseResources;
    }

    protected Range getRange() {
        return m_range;
    }

    protected void setRange(Range m_range) {
        this.m_range = m_range;
    }

}
