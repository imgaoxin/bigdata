package docinvertindex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class InvertIndexCombiner extends Reducer<Text, Text, Text, Text> {

    //private Text cKey = new Text();
    private Text cValue = new Text();

    /**
     * combiner本地聚合,输出<word,"path->count">
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //String[] wordPath = key.toString().trim().split("->");
        //String word = wordPath[0];
        //String path = wordPath[1];

        int count = 0;
        //通过values迭代器,遍历kv中所有的value进行累加
        for (Text value : values) {
            count += Integer.parseInt(value.toString());
        }
        //cKey.set(word);
        //cValue.set(path + "->" + count);
        cValue.set("" + count);
        //输出统计结果
        //context.write(cKey, cValue);
        context.write(key, cValue);
    }
}