package wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    /**
     * 将shuffle阶段分发过来的kv数据进行聚合，聚合的机制是相同key的kv对聚合为一组
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
        //词频少于3次的数据不显示输出
        if (count >= 3) {
            result.set(count);
            //输出这个单词的统计结果
            context.write(key, result);
        }
    }
}