package definitive_guide.chapter3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;

/**
 * 将本地文件复制到Hadoop文件系统
 * FSDataOutputStream不允许在文件定位，这是因为hdfs只允许在文件末尾追加
 *
 */
public class FileCopyWithProgress {
    public static void main(String[] args) throws IOException {
        String localSrc=args[0];
        String dst = args[1];
        InputStream in  = new BufferedInputStream(new FileInputStream(localSrc));
        Configuration conf=new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dst), conf);
//        fs.mkdirs(new Path(""));创建目录
        OutputStream out=fs.create(new Path(dst), new Progressable() {
            @Override
            public void progress() {
                System.out.print(".");
            }
        });

        IOUtils.copyBytes(in,out,4096,true);
    }
}
