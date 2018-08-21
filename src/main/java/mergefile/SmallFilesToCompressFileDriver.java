package mergefile;

import com.hadoop.compression.lzo.LzopCodec;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by wangbin10 on 2018/7/30.
 */
public class SmallFilesToCompressFileDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf(), "Merge smallFiles");
        if (job == null) {
            return -1;
        }
        job.setJarByClass(SmallFilesToCompressFileDriver.class);
        /**设置输入格式,默认的输入是key=long,value=text*/
        job.setInputFormatClass(WholeFileInputFormat.class);
        /**设置输出格式,默认的输出是key=long,value=text*/
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        /**设置输入输出路径*/
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        /**在输出中使用压缩，压缩格式Lzo*/
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, LzopCodec.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SmallFilesToCompressFileDriver(), args);
        System.exit(exitCode);
    }
}
