package wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountJob {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length < 2) {
            System.err.println("Usage: Count <in> <out>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job wordCountJob = Job.getInstance(conf);
        wordCountJob.setJobName("WordCount");

        //重要：指定本job所在的jar包
        wordCountJob.setJarByClass(WordCountJob.class);
        //设置mapper
        wordCountJob.setMapperClass(WordCountMapper.class);
        //设置Combiner
        wordCountJob.setCombinerClass(WordCountCombiner.class);
        //设置reducer
        wordCountJob.setReducerClass(WordCountReducer.class);
        //设置map阶段输出的kv数据类型
        wordCountJob.setMapOutputKeyClass(Text.class);
        wordCountJob.setMapOutputValueClass(IntWritable.class);
        //设置最终输出的kv数据类型
        wordCountJob.setOutputKeyClass(Text.class);
        wordCountJob.setOutputValueClass(IntWritable.class);

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
        FileInputFormat.setInputPaths(wordCountJob, inPath);
        FileOutputFormat.setOutputPath(wordCountJob, outPath);
        //提交job给hadoop集群
        System.exit(wordCountJob.waitForCompletion(true) ? 0 : 1);
    }
}