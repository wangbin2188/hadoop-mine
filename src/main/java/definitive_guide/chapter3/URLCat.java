package definitive_guide.chapter3;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/**
 * 将hdfs文件内容打印到标准输出
 * 脚本：
 * export HADOOP_CLASSPATH=hadoop-examples.jar
 * Hadoop URLCat hdfs://localhost/user/tom/quangle.txt
 */
public class URLCat {
    static {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    public static void main(String[] args) {
        InputStream in=null;
        try {
            in=new URL(args[0]).openStream();
            IOUtils.copyBytes(in,System.out,4096,false);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(in);
        }
    }
}
