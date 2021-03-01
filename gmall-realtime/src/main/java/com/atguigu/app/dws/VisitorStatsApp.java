package com.atguigu.app.dws;

import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Mock -> Nginx -> Logger ->
 * Kafka(ods_base_log) ->
 * FlinkApp(LogBaseApp) ->
 * Kafka(dwd_page_log dwd_start_log dwd_display_log) ->
 * FlinkApp(UvApp UserJumpApp) ->
 * Kafka(dwm_unique_visit dwm_user_jump_detail) ->
 * VisitorStatsApp
 */
public class VisitorStatsApp {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //1.1 设置状态后端
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/dwd_log/ck"));
        //1.2 开启CK
//        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000L);

        //2.读取Kafka主题的数据
        // dwd_page_log(pv,访问时长,进入页面数)
        // dwm_unique_visit(uv)
        // dwm_user_jump_detail (跳出数)
        String groupId = "visitor_stats_app";
        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";
        DataStreamSource<String> pageLogDS = env.addSource(MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId));
        DataStreamSource<String> uvDS = env.addSource(MyKafkaUtil.getKafkaSource(uniqueVisitSourceTopic, groupId));
        DataStreamSource<String> userJumpDS = env.addSource(MyKafkaUtil.getKafkaSource(userJumpDetailSourceTopic, groupId));

        //测试打印
        pageLogDS.print("Page>>>>>>>>>>>>>>");
        uvDS.print("UV>>>>>>>>>>>>>>>>>>>>>");
        userJumpDS.print("UserJump>>>>>>>>>");

        //3.格式化流数据,使其字段统一  (JavaBean)

        //4.将多个流的数据进行union

        //5.分组,聚合计算

        //6.将聚合之后的数据写入ClickHouse

        //7.执行任务
        env.execute();

    }

}
