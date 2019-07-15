package Util;

import Tests.Property;
import org.bytedeco.javacv.CanvasFrame;

import javax.swing.*;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.Serializable;

public class ImageViewer implements Serializable {
    static transient private CanvasFrame canvas;
    static transient private String title;
    public ImageViewer(String title) {
        this.title = title;
        canvas = new CanvasFrame(title);//新建一个窗口
        canvas.setCanvasSize(Property.WIDTH,Property.HEIGHT);
        canvas.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    }

    public void showImage(Image image)
    {
        canvas.showImage(image);
    }

    public void showImage(int[] data) {
        BufferedImage image = new BufferedImage(Property.WIDTH, Property.HEIGHT, BufferedImage.TYPE_INT_ARGB);
        image.setRGB(0, 0, Property.WIDTH, Property.HEIGHT, data, 0, Property.WIDTH);

        canvas.showImage(image);
    }

    public void showImage(String[] data) {

        //图像处理
        String[] tempdata = data;

        int[] ints = new int[tempdata.length];
        for (int i = 0; i < tempdata.length; i++) {
            ints[i] = Integer.parseInt(tempdata[i]);
        }
        showImage(ints);
    }

    public void updateInfo(long delay, long fps) {
        if(fps != 0) {
            int realFps = (int) (1000 / fps);
            canvas.setTitle(title + " ping:" + delay + "ms," + " fps:" + realFps);
        }
    }

}
