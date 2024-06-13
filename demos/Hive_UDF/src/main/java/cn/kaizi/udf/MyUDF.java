package cn.kaizi.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class MyUDF extends UDF {
    // 模拟Hive的内置函数upper方法
    // 将字符串的第一个字符转为大写，而其它字符不改变
    public Text evaluate(final Text line) {
        if (line.toString() != null && !line.toString().equals("")) {
            String str = line.toString().substring(0, 1).toUpperCase() + line.toString().substring(1);
            return new Text(str);
        }

        return new Text("");
    }
}
