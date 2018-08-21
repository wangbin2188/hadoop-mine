mergeFun(){
output=/tmp/logs/shtermuser/compress/
input=/tmp/logs/shtermuser/origin/-1532926800000
hadoop fs -rm -r ${output}
hadoop jar hadoopMax-1.jar mergefile.SmallFilesToCompressFileDriver \
-D mapred.reduce.tasks=10 \
${input} ${output}
}

maxFun(){
input=/tmp/logs/shtermuser/sample.txt
output=/tmp/logs/shtermuser/output
hadoop fs -rm -r ${output}
hadoop jar hadoopMax-1.jar maxtemperature.MaxTemperatureDriver \
-D mapred.reduce.tasks=1 \
${input} ${output}
}

hbaseFun(){
input=/tmp/logs/shtermuser/sample.txt
hadoop jar hadoopMax-1.jar hbaseimport.HBaseTemperatureImporter \
-libjars hbase-server-1.3.0.jar \
${input}
}

seqWriteFun(){
output=/tmp/logs/shtermuser/sample.seq
export HADOOP_CLASSPATH=hadoopMax-1.jar
hadoop  ch05_io.SequenceFileWriteDemo ${output}
}

seqReadFun(){
input=/tmp/logs/shtermuser/sample.seq
export HADOOP_CLASSPATH=hadoopMax-1.jar
hadoop ch05_io.SequenceFileReadDemo ${input}
}

seqReadFun
