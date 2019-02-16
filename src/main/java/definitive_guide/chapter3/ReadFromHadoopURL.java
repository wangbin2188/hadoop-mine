package definitive_guide.chapter3;

import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

public class ReadFromHadoopURL {
    public static void main(String[] args) {
        InputStream in =null;
        try {
            in= new URL("hdfs://host/path").openStream();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(in);
        }
    }
}
