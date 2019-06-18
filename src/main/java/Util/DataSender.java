package Util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class DataSender extends Thread {
    private Thread thread;
    private String threadName;

    public DataSender(String name)
    {
        threadName = name;
    }

    public void run() {
        Process p;
        OutputStreamWriter outputStreamWriter = null;
        BufferedWriter writer = null;
        int port = 9998;
        try {
            p = Runtime.getRuntime().exec("cmd /c nc -l -p " + String.valueOf(port), null, new File("C:\\"));
            //p.waitFor();
            outputStreamWriter = new OutputStreamWriter(p.getOutputStream(), "GBK");
            writer = new BufferedWriter(outputStreamWriter);
            while (true) {
                outputStreamWriter.write((int)(1+Math.random()*(100-1+1)));
                //outputStreamWriter.flush();
                //System.out.println(1);
                Thread.sleep(10);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            try {
                writer.close();
                outputStreamWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void start () {
        System.out.println("Starting " +  threadName );
        if (thread == null) {
            thread = new Thread (this, threadName);
            thread.start ();
        }
    }
}
