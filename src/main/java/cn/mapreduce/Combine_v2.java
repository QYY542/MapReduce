package cn.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Combine_v2 extends Reducer<Text, Text, Text, Text> {
    private static Text word = new Text();
    private static Text index = new Text();

    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws java.io.IOException, InterruptedException {
//          对key进行操作， 截取分开 单词 和 URI
        String[] splits = key.toString().split(":");
        if (splits.length != 2) {
            return;
        }
        //统计位置
        StringBuilder position = new StringBuilder("(");

//            统计词频
        long count = 0;
        for (Text v : values) {
            count++;
            position.append(v.toString() + ",");
        }

        position.deleteCharAt(position.length() - 1);

        position.append(")");

//            设置key 为 splits[0] 单词  value 为 splits[1] 文件名 + 次数
        word.set(splits[0]);
//        index.set(splits[1] + ":" + count + position);
        index.set(splits[1] + ":" + count);
        context.write(word, index);
    }

}
