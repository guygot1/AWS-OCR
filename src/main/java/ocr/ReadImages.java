package ocr;

import net.sourceforge.tess4j.ITesseract;
import net.sourceforge.tess4j.Tesseract;
import net.sourceforge.tess4j.TesseractException;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;

public class ReadImages {
    public static void main(String [] args){
        String imgurl = "https://www.askideas.com/110-most-inspirational-diet-quotes-and-sayings/its-not-about-perfect-its-about-effort-and-when-you-bring-that-effort-every-single-day-thats-where-transformation-happens-thats-how-change-occurs-jillian-michaels-2/";
        ITesseract image = new Tesseract();
        try(InputStream in = new URL(imgurl).openStream()){
            Files.copy(in, Paths.get("C:\\Users\\guy\\Desktop\\mevu\\images\\image2.png"));
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try{
          String str =  image.doOCR(new File("C:\\Users\\guy\\Desktop\\mevu\\images\\image2.png"));
            System.out.println("Data from image is "+ str);
        } catch ( TesseractException e){
            System.out.println("Exception " + e.getMessage());
        }

    }
    }

