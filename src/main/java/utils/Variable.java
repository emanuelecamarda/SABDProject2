package utils;

public class Variable {

    public static final String  REDIS_DATA		            = "data";
    public static final String  REDIS_CONSUMED 	            = "consumed";
    public static final String  REDIS_COUNTER_1HOUR         = "commentCounterStartWindow1Hour";
    public static final String  REDIS_COUNTER_24HOUR        = "commentCounterStartWindow24Hour";
    public static final String  REDIS_COUNTER_7DAY          = "commentCounterStartWindow7Day";
    public static final String  REDIS_INTER_RANKER_1HOUR    = "IntermediateRankingTimestamp1Hour";
    public static final String  REDIS_INTER_RANKER_24HOUR   = "IntermediateRankingTimestamp24Hour";
    public static final String  REDIS_INTER_RANKER_7DAY     = "IntermediateRankingTimestamp7Day";
    public static final String  REDIS_INTER_COUNTER_24HOUR  = "IntermediateCounterTimestamp24Hour";
    public static final String  REDIS_INTER_COUNTER_7DAY    = "IntermediateCounterTimestamp7Day";
    public static final String  REDIS_INTER_COUNTER_1MONTH  = "IntermediateCounterTimestamp1Month";
    public static final int     SHORT_SLEEP                 = 10;
    public static final String  FILE                        = "./Comments_jan-apr2018.csv";
    public static final String  REDIS_EOF                   = "EOF";
    public static final int     TOP_Q1                      = 3;
    public static final String RABBITMQ_QUEUE_Q1_1HOUR      = "resultQ1Window1Hour";
    public static final String RABBITMQ_QUEUE_Q1_24HOUR     = "resultQ1Window24Hour";
    public static final String RABBITMQ_QUEUE_Q1_7DAY       = "resultQ1Window7Day";
    public static final String RABBITMQ_QUEUE_Q2_24HOUR     = "resultQ1Window24Hour";
    public static final String RABBITMQ_QUEUE_Q2_7DAY       = "resultQ1Window7Day";
    public static final String RABBITMQ_QUEUE_Q2_1MONTH     = "resultQ1Window1Month";
    public static final String OUTPUT_FILE_Q1_1HOUR         = "./output/outputQ1_1Hour.csv";
    public static final String OUTPUT_FILE_Q1_24HOUR        = "./output/outputQ1_24Hour.csv";
    public static final String OUTPUT_FILE_Q1_7DAY          = "./output/outputQ1_7Day.csv";
    public static final String OUTPUT_FILE_Q2_24HOUR        = "./output/outputQ2_24Hour.csv";
    public static final String OUTPUT_FILE_Q2_7DAY          = "./output/outputQ2_7Day.csv";
    public static final String OUTPUT_FILE_Q2_1MONTH        = "./output/outputQ2_1Month.csv";
    public static final String HEADER_QUERY1                = "ts,artID_1,nCmnt_1,artID_2,nCmnt_2,artID_3,nCmnt_3";
    public static final String HEADER_QUERY2                = "ts,count_h00,count_h02,count_h04,count_h06,count_h08," +
            "count_h10,count_h12,count_h14,count_h16,count_h18,count_h20,count_h22";

}
