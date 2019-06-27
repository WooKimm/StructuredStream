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
        int port = 9999;
        try {
            p = Runtime.getRuntime().exec("cmd /k nc -l -p " + String.valueOf(port));
            //p.waitFor();
            outputStreamWriter = new OutputStreamWriter(p.getOutputStream(), "UTF-8");
            writer = new BufferedWriter(outputStreamWriter);
            while (true) {
                String num = String.valueOf((int)(1+Math.random()*(100-1+1)));
                writer.write(num);
                writer.write("\n");
                writer.flush();

                Thread.sleep(100);
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
