package definitive_guide.chapter3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class FileSystemWriteDemo {
    public static void main(String[] args) throws IOException {
        /**
         * 新建一个文件后，能在文件系统立即可见
         */
        Path p = new Path("p");
        FileSystem fs = FileSystem.get(new Configuration());
        fs.exists(p);

        /**
         * 写入文件的内容并不能保证立即可见，即使刷新并存储
         * 总之，当前写入的块对其他reader不可见
         * hdfs提供了一种强行将所有缓存刷新到datanode的方法hflush
         * hflush返回成功后，hdfs能够保证写入文件的数据对所有新reader可见
         * 但hflush不能保证数据以及写入磁盘，仅确保在datanode内存中
         * 为确保写入磁盘，可以用hsync方法替代
         * close方法暗含了hflush方法
         */
        FSDataOutputStream out = fs.create(p);
        out.write("content".getBytes("UTF-8"));
        out.flush();
        fs.getFileStatus(p).getLen();
        out.hflush();
        fs.getFileStatus(p).getLen();
        out.hsync();
        out.close();
    }
}
