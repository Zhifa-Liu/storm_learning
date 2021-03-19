package cn.edu.neu.advertise;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;
import org.apache.storm.windowing.TupleWindow;

import java.awt.*;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Array;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author 32098
 * 最近1分钟广告总点击量,每10s计算一次
 */
public class ClickNearlyMinuteBoltB extends BaseWindowedBolt {
    private OutputCollector collector;
    private String projectRoot;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.projectRoot = System.getProperty("user.dir");
    }

    @Override
    public void execute(TupleWindow tupleWindow) {
        Map<String, Integer> hmsClick = new TreeMap<>();

        for (Tuple tuple: tupleWindow.get()){

            String ts = tuple.getStringByField("time_stamp");
            String aid = tuple.getStringByField("aid");

            long timeStamp = Long.parseLong(ts.trim());
            String time = new SimpleDateFormat("HH:mm:ss").format(new Date(timeStamp));
            String[] hms = time.split(":");
            int s = (Integer.parseInt(hms[2])/10+1)*10;
            int m = Integer.parseInt(hms[1]);
            int h = Integer.parseInt(hms[0]);
            if(s == 60){
                m = m + 1;
                s = 0;
                if(m == 60){
                    h = h + 1;
                    if(h == 24){
                        h = 0;
                    }
                }
            }
            String hStr, mStr, sStr;
            if(h < 10){
                hStr = "0" + h;
            }else{
                hStr = String.valueOf(h);
            }
            if(m < 10){
                mStr = "0" + m;
            }else{
                mStr = String.valueOf(m);
            }
            if(s == 0){
                sStr = "00";
            }else{
                sStr = String.valueOf(s);
            }

            String hms_ = hStr+":"+mStr+":"+sStr;
            if(hmsClick.containsKey(hms_)){
                hmsClick.put(hms_, hmsClick.get(hms_)+1);
            }else{
                hmsClick.put(hms_, 1);
            }
        }
        String file = projectRoot + "/src/main/java/cn/edu/neu/advertise/advertise_click_nearly_minute.json";
        try {
            PrintWriter out = new PrintWriter(new FileWriter(new File(file), false));
            StringBuffer jsonStr = new StringBuffer("[");
            hmsClick.forEach(
                    (xtime, yclick) -> {
                        String jsonElem = "{\"xtime\":\""+xtime+"\",\"yclick\":\""+yclick+"\"},";
                        System.out.println(jsonElem);
                        jsonStr.append(jsonElem);
                    }
            );
            jsonStr.deleteCharAt(jsonStr.length()-1);
            jsonStr.append("]");
            out.println(jsonStr.toString());
            out.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
