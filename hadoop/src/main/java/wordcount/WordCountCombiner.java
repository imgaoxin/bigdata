package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable cValue = new IntWritable();

    /**
     * combiner本地聚合
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //定义计数器
        int count = 0;
        //通过values迭代器,遍历kv中所有的value进行累加
        for (IntWritable value : values) {
            count += value.get();
        }
        cValue.set(count);
        //输出这个单词的统计结果
        context.write(key, cValue);
    }
}