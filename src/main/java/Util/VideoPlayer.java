package Util;

import Util.ImageViewer;
import Util.VideoWriter;

public class VideoPlayer extends Thread {
    /*
    private Thread thread;
    private String threadName;
    ImageViewer viewer;

    public VideoPlayer(String name)
    {
        threadName = name;
        viewer = new ImageViewer("ImageViewer");
    }

    @Override
    public void run()
    {
        while (true)
        {
            if(!VideoWriter.isFirstStart || VideoWriter.rows.size()>0)
            {
                String str = VideoWriter.rows.poll().get(0).toString();
                // pass the value
                //      compare.frame = str

                String[] data = str.split(";");
                //      val newData = ImageProcess.watermark(ImageProcess.toGray(data))
                viewer.showImage(data);
                if(VideoWriter.rows.size() == 0)
                {
                    VideoWriter.isFirstStart = true;
                }
                else
                {
                    VideoWriter.isFirstStart = false;
                }
                try {
                    Thread.sleep(70);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void start () {
        System.out.println("Starting " +  threadName );
        if (thread == null) {
            thread = new Thread (this, threadName);
            thread.start ();
        }
    }
    */
}
