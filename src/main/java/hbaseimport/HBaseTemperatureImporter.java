package hbaseimport;


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by wangbin10 on 2018/7/30.
 * 目前尚有TableOutputFormat not found的错误未解决！！！
 */
public class HBaseTemperatureImporter extends Configured implements Tool {
    static class HBaseTemperatureMapper<K> extends Mapper<LongWritable, Text, K, Put> {
        private NcdcRecordParser parser = new NcdcRecordParser();

        @Override
        public void map(LongWritable key, Text value, Context context) throws  IOException, InterruptedException {
            parser.parse(value.toString());
            if (parser.isValidTemperature()) {
                byte[] rowKey = RowKeyConverter.makeObservationRowKey(parser.getStationId(),parser.getObservationDate().getTime());
                Put p = new Put(rowKey);
                p.add(HBaseTemperatureQuery.DATA_COLUMNFAMILY,HBaseTemperatureQuery.AIRTEMP_QUALIFIER,Bytes.toBytes(parser.getAirTemperature()));
                context.write(null, p);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int exitCode= ToolRunner.run(new HBaseTemperatureImporter(),args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage: HBaseTemperatureImporter <input>");
            return -1;
        }
        Job job = new Job(getConf(), getClass().getSimpleName());
        job.setJarByClass(getClass());
        /**设置输入路径*/
        FileInputFormat.addInputPath(job, new Path(args[0]));
        /**设置hbase输出表名*/
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "observations");
        job.setMapperClass(HBaseTemperatureMapper.class);
        job.setNumReduceTasks(0);
        /**设置输出类型*/
        job.setOutputFormatClass(TableOutputFormat.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
