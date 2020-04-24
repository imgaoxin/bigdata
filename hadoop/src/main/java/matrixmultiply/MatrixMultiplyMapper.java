package matrixmultiply;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MatrixMultiplyMapper extends Mapper<Object, Text, Text, Text> {

    //文件名,标识某个矩阵
    private String flag = null;
    // 矩阵A的行数
    private int rowA = 0;
    // 矩阵B的列数
    private int columnB = 0;

    private Text mapKey = new Text();
    private Text mapValue = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) context.getInputSplit();
        flag = split.getPath().getName();

        Configuration conf = context.getConfiguration();
        rowA = Integer.parseInt(conf.get("rowA"));
        columnB = Integer.parseInt(conf.get("columnB"));
    }

    /**
     * Map输出类型为<"结果矩阵行,列"，"矩阵名,索引号,值">
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] val = value.toString().trim().split(",");
        if (val.length != 3) {
            throw new RuntimeException("MatrixMapper Error!");
        }
        if ("ma".equals(flag)) {
            for (int i = 1; i <= columnB; i++) {
                mapKey.set(val[0] + "," + i);
                mapValue.set("a," + val[1] + "," + val[2]);
                context.write(mapKey, mapValue);
            }
        } else if ("mb".equals(flag)) {
            for (int i = 1; i <= rowA; i++) {
                mapKey.set(i + "," + val[1]);
                mapValue.set("b," + val[0] + "," + val[2]);
                context.write(mapKey, mapValue);
            }
        }
    }
}