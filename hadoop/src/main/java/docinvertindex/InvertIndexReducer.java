package docinvertindex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class InvertIndexReducer extends Reducer<Text, Text, Text, Text> {

    private Map<String, Integer> map = new HashMap<String, Integer>();
    private String keyWord = null;

    private Text result = new Text();

    /**
     * 聚合的机制是相同key的kv对聚合为一组
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String[] keys = key.toString().trim().split("->");
        String currWord = keys[0];
        String docPath = keys[1];

        if (keyWord == null) {
            keyWord = currWord;
        }

        if (!keyWord.equals(currWord)) {
            wordIsNotEqualsToWrite(context);
            keyWord = currWord;
        }

        int newV = 0;
        for (Text val : values) {
            newV += Integer.parseInt(val.toString().trim());
        }
        Integer oldV = map.get(docPath);
        map.put(docPath, (oldV == null ? 0 : oldV) + newV);
    }

    /**
     * word不同时,将上一个统计好的word结果输出
     *
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    private void wordIsNotEqualsToWrite(Context context) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            sb.append(entry.getKey()).append("->").append(entry.getValue()).append(";");
        }
        result.set(sb.toString());
        context.write(new Text(keyWord), result);
        map.clear();
    }

    @Override
    protected void cleanup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        wordIsNotEqualsToWrite(context);
    }
}