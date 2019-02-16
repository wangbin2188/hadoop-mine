package definitive_guide.chapter3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.IOException;
import java.net.URI;

/**
 * 用于排除匹配正则表达式的路径
 */
public class RegexExcludePathFilter implements PathFilter {
    private final String regex;

    public RegexExcludePathFilter(String regex) {
        this.regex = regex;
    }


    @Override
    public boolean accept(Path path) {
        return !path.toString().matches(regex);
    }


    /**
     * 选出匹配正则表达式的文件，并过滤掉不符合条件的文件
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        String uri = args[0];
        Configuration conf= new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        fs.globStatus(new Path("/2007/*/*"),new RegexExcludePathFilter("^./*2007/12/31$"));

        Path file = new Path("");

        /**
         * 判断文件和目录是否存在
         */
        fs.exists(file);
        /**
         * 列出目录中的内容
         */
        fs.listFiles(file,true);

        /**
         * 永久性删除文件和目录,第二个参数为true时，非空目录及其内容才会被删除
         */
        fs.delete(file,true);

    }
}

