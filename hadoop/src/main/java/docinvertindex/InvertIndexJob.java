package docinvertindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class InvertIndexJob {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length < 3) {
            System.err.println("Usage: Count <in> <out> <stopwordspath>");
            System.exit(2);
        }

        Configuration conf = new Configuration();

        //第2个参数表示停用词表路径
        Path stopWordsPath = new Path(args[2]);
        conf.set("stopWordsPath", stopWordsPath.toString());

        Job invertIndexJob = Job.getInstance(conf);
        invertIndexJob.setJobName("DocInvertIndex");

        //重要：指定本job所在的jar包
        invertIndexJob.setJarByClass(InvertIndexJob.class);
        //设置reducer tasks num
        invertIndexJob.setNumReduceTasks(4);
        //设置mapper
        invertIndexJob.setMapperClass(InvertIndexMapper.class);
        //设置partitioner
        invertIndexJob.setPartitionerClass(InvertIndexPartitioner.class);
        //设置Combiner
        invertIndexJob.setCombinerClass(InvertIndexCombiner.class);
        //设置reducer
        invertIndexJob.setReducerClass(InvertIndexReducer.class);
        //设置map阶段输出的kv数据类型
        invertIndexJob.setMapOutputKeyClass(Text.class);
        invertIndexJob.setMapOutputValueClass(Text.class);
        //设置最终输出的kv数据类型
        invertIndexJob.setOutputKeyClass(Text.class);
        invertIndexJob.setOutputValueClass(Text.class);

        //第0个参数是输入目录,第1个表示输出目录
        Path inPath = new Path(args[0]);
        Path outPath = new Path(args[1]);

        // 判断output文件夹是否存在，如果存在则删除
        FileSystem fileSystem = outPath.getFileSystem(conf);
        if (fileSystem.exists(outPath)) {
            // true的意思是，就算output有东西，也一带删除
            fileSystem.delete(outPath, true);
        }

        //设置要处理的文本数据所存放的路径
        FileInputFormat.setInputPaths(invertIndexJob, inPath);
        FileOutputFormat.setOutputPath(invertIndexJob, outPath);
        //提交job给hadoop集群
        System.exit(invertIndexJob.waitForCompletion(true) ? 0 : 1);
    }
}