package definitive_guide.chapter3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public class FileSystemCat {
    public static void main(String[] args) throws IOException {
        String uri = args[0];
        Configuration conf = new Configuration();
        /**
         * FileSystem的工厂方法
         * 同时可以获取本地文件系统：
         * LocalFileSystem local = FileSystem.getLocal(conf);
         */
        FileSystem fs = FileSystem.get(URI.create(uri), conf);

        InputStream in=null;
        /**
         * open方法获取文件输入流
         */
        try {
            in=fs.open(new Path(uri));
            IOUtils.copyBytes(in,System.out,4096,false);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(in);
        }

    }
}
