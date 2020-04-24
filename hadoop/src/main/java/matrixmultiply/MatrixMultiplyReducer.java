package matrixmultiply;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MatrixMultiplyReducer extends Reducer<Text, Text, Text, IntWritable> {

    //矩阵A的列(即矩阵B的行)
    private int columnA = 0;
    private IntWritable result = new IntWritable();

    @Override
    protected  void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        columnA = Integer.parseInt(conf.get("columnA"));
    }

    /**
     * 矩阵乘法计算
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String, String> ma = new HashMap<String, String>(columnA);
        Map<String, String> mb = new HashMap<String, String>(columnA);

        for (Text value : values) {
            String[] val = value.toString().split(",");
            if (val.length != 3) {
                throw new RuntimeException("MatrixReducer Error!");
            }
            if ("a".equals(val[0])) {
                ma.put(val[1], val[2]);
            } else if ("b".equals(val[0])) {
                mb.put(val[1], val[2]);
            }
        }

        int sum = 0;
        Set<String> keys = ma.keySet();
        for (String k : keys) {
            // 如果输入文件矩阵是稀疏存储(不存储0值),可以将相关计算对排除,不影响结果
            if (mb.get(k) == null) {
                continue;
            }
            sum += Integer.parseInt(ma.get(k)) * Integer.parseInt(mb.get(k));
        }
        result.set(sum);
        context.write(key, result);
    }
}