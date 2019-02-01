import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by wangbin10 on 2019/2/1.
 * hive>add jar /home/shtermuser/scale/shell/hive_udf-1.jar;
 * hive>create temporary function get_month as 'UDFGetMonthName';
 * hive>select get_month('2019-11-01');
 * OK
 * 10
 */
public class UDFGetMonthName extends UDF {
    private SimpleDateFormat format;

    public UDFGetMonthName() {
        this.format = new SimpleDateFormat("yyyy-MM-dd");
    }

    public String evaluate(String date) throws ParseException {
        Date dt = format.parse(date);
        int month = dt.getMonth();
        return String.valueOf(month);
    }
}
