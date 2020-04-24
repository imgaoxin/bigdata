package matrixmultiply;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MatrixMultiplyJob {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args == null || args.length < 5) {
            System.err.println("Usage: Count <in> <out> <rowA> <columnA> <columnB>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        conf.set("rowA",args[2]);
        conf.set("columnA",args[3]);
        conf.set("columnB",args[4]);
        Job matrixMultiplyJob = Job.getInstance(conf);
        matrixMultiplyJob.setJobName("MatrixMultiply");

        //重要：指定本job所在的jar包
        matrixMultiplyJob.setJarByClass(MatrixMultiplyJob.class);
        //设置mapper
        matrixMultiplyJob.setMapperClass(MatrixMultiplyMapper.class);
        //设置reducer
        matrixMultiplyJob.setReducerClass(MatrixMultiplyReducer.class);
        //设置map阶段输出的kv数据类型
        matrixMultiplyJob.setMapOutputKeyClass(Text.class);
        matrixMultiplyJob.setMapOutputValueClass(Text.class);
        //设置最终输出的kv数据类型
        matrixMultiplyJob.setOutputKeyClass(Text.class);
        matrixMultiplyJob.setOutputValueClass(IntWritable.class);

        //第0个参数是输入目录,第1个表示输出目录
        Path inPath = new Path(args[0]);
        Path outPath = new Path(args[1]);

        // 判断output文件夹是否存在，如果存在则删除
        FileSystem fileSystem = outPath.getFileSystem(conf);
        if (fileSystem.exists(outPath)) {
            // true的意思是，就算output有东西也删除
            fileSystem.delete(outPath, true);
        }

        //设置要处理的文本数据所存放的路径
        FileInputFormat.setInputPaths(matrixMultiplyJob, inPath);
        FileOutputFormat.setOutputPath(matrixMultiplyJob, outPath);
        //提交job给hadoop集群
        System.exit(matrixMultiplyJob.waitForCompletion(true) ? 0 : 1);
    }
}