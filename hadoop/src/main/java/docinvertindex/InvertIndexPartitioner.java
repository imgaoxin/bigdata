package docinvertindex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class InvertIndexPartitioner extends HashPartitioner<Text, Text> {
    @Override
    public int getPartition(Text key, Text value, int numReduceTasks) {
        String k = key.toString();
        String[] splits = k.split("->");
        return super.getPartition(new Text(splits[0]), value, numReduceTasks);
    }
}
