package definitive_guide.chapter3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * 展示文件状态信息
 */
public class ShowFileStatusTest {
    private static Configuration conf;
    private static Path file;
    static{

        conf = new Configuration();
        file = new Path("");

    }

    public static void main(String[] args) throws IOException {
        try (FileSystem fs = FileSystem.get(conf)) {
            FileStatus stat = fs.getFileStatus(file);
            stat.getPath();
            stat.isDirectory();
            stat.getLen();
            stat.getModificationTime();
            stat.getReplication();
            stat.getBlockSize();
            stat.getOwner();
            stat.getGroup();
            stat.getPermission();

        }
    }
}
