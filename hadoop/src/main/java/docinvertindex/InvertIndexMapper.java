package docinvertindex;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class InvertIndexMapper extends Mapper<Object, Text, Text, Text> {

    //文档路径
    private String docPath = null;
    //stop words list
    private Set<String> stopWords = null;

    private Text mKey = new Text();
    private final static Text one = new Text("1");

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        docPath = inputSplit.getPath().toString();

        Configuration conf = context.getConfiguration();
        String stopWordsPath = conf.get("stopWordsPath");
        stopWords = new HashSet<String>();
        FSDataInputStream fsDataInputStream = null;
        BufferedReader reader = null;
        try {
            fsDataInputStream = FileSystem.get(conf).open(new Path(stopWordsPath));
            reader = new BufferedReader(new InputStreamReader(fsDataInputStream));
            String tmp;
            while ((tmp = reader.readLine()) != null) {
                String[] words = tmp.trim().split(" ");
                stopWords.addAll(Arrays.asList(words));
            }
        } catch (IOException e) {
            throw new RuntimeException("停用词文件读取异常", e);
        } finally {
            if (reader != null) {
                reader.close();
            }
            if (fsDataInputStream != null) {
                fsDataInputStream.close();
            }
        }
    }

    /**
     * Map输出<"word,path"，1>
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
            //输出<"word,path"，1>
            for (String word : words) {
                //stop words
                if (!stopWords.contains(word)) {
                    mKey.set(word + "->" + docPath);
                    context.write(mKey, one);
                }
            }
        }
    }
}