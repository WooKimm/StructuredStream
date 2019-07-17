package sparkcl;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class ProcessSync
{

    public static class ExclusiveAccess implements AutoCloseable
    {
        //FileChannel m_fileChannel;
        String m_lockName;
        FileLock m_lock;

        public ExclusiveAccess()
        {
        }

        public ExclusiveAccess(String lockName, long timeOutMilli) throws Exception
        {
            setLockName(lockName);
            gainExclusiveAccess(timeOutMilli);
        }

        private String setLockName(String lockName)
        {
            return m_lockName = lockName;
        }

        private void gainExclusiveAccess(long timeOutMilli)
                throws Exception
        {
            logMsg("Trying to Acquire lock");
            boolean result = waitForLock(timeOutMilli);
            if(result)
            {
                logMsg("Success Acquiring lock");
            }
            else
            {
                logMsg("Failed to Acquire lock");
                throw new Exception("Failed to Acquire lock: " +  m_lockName);
            }
        }

        private String getIdInfoStr()
        {
            return new String(" => lockName = " +  m_lockName  + " => TID = " + Thread.currentThread().getId());
        }

        private void logMsg(String msg)
        {
            System.out.println("ExclusiveAccess -> " + msg + getIdInfoStr() );
        }

        private void displayException(Exception e)
        {
            //e.printStackTrace();
            System.out.println("ExclusiveAccess -> Exception: " + e.getStackTrace()[0] + getIdInfoStr());
        }

        public boolean waitForLock(long timeOutMilli)
        {
            final long PollTimeMilli = 250;
            //System.out.println("ExclusiveAccess -> Trying to Acquire lock...");
            while ((m_lock = tryLock(m_lockName))==null)
            {
                timeOutMilli-=PollTimeMilli;
                if(timeOutMilli<=0)
                    break;
                try {
                    Thread.sleep(PollTimeMilli);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    //e.getMessage();
                    displayException(e);
                }
            }

            if(m_lock!=null)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        FileLock tryLock(String fileName)
        {
            try
            {
                //fileChannel = createFile(fileName);
                m_lock = createFile(fileName).tryLock();
                return m_lock;
            }
            catch (Exception e)
            {
                displayException(e);
                return null;
            }
        }

        public void releaseLock()
        {
            try {
                m_lock.release();
            } catch (IOException e) {
                displayException(e);
            }
        }

        private FileChannel createFile(String fileName) throws IOException
        {
            Path path = Paths.get(fileName);
            return FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
        }

        @Override
        public void close() throws Exception
        {
            logMsg("Released lock");
            releaseLock();
        }
    }
}

