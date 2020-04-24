package wordcount;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

    private Text mKey = new Text();
    private final static IntWritable one = new IntWritable(1);

    /**
     * Map输出类型为<单词，出现次数>
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        //将输入的一行文本，转换成String 类型
        String line = value.toString();
        if (StringUtils.isNotBlank(line)) {
            //将这行文本按" "切分成单词
            String[] words = line.trim().split(" ");
            //输出<单词，1>
            for (String word : words) {
                //匹配英文字母开头的单词
                if (word.matches("^[a-zA-Z][\\s\\S]*$")) {
                    mKey.set(word);
                    context.write(mKey, one);
                }
            }
        }
    }
}