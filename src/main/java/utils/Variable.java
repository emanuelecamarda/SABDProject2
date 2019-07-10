package utils;

public class Variable {

    public static final String  REDIS_DATA		            = "data";
    public static final String  REDIS_CONSUMED 	            = "consumed";
    public static final String  REDIS_COMMENT_COUNTER       = "commentCounterStartWindow";
    public static final String  REDIS_INTER_RANKER          = "IntermediateRankingTimestamp";
    public static final int     SHORT_SLEEP                 = 10;
    public static final String  FILE                        = "./Comments_jan-apr2018.csv";
    public static final String  REDIS_EOF                   = "EOF";
    public static final int     TOP_Q1                      = 3;
    public static final int     TOP_Q2                      = 10;
    public static final String RABBITMQ_QUEUE_Q1_1HOUR      = "resultQ1Window1Hour";
    public static final String RABBITMQ_QUEUE_Q1_24HOUR     = "resultQ1Window24Hour";
    public static final String RABBITMQ_QUEUE_Q1_7DAY       = "resultQ1Window7Day";
    public static final String RABBITMQ_QUEUE_Q2_24HOUR     = "resultQ1Window24Hour";
    public static final String RABBITMQ_QUEUE_Q2_7DAY       = "resultQ1Window7Day";
    public static final String RABBITMQ_QUEUE_Q2_1MONTH     = "resultQ1Window1Month";
    public static final String OUTPUT_FILE_Q1_1HOUR         = "./output/outputQ1_1Hour.csv";
    public static final String OUTPUT_FILE_Q1_24HOUR        = ".../outputQ1_24Hour.csv";
    public static final String OUTPUT_FILE_Q1_7DAY          = ".../outputQ1_7Day.csv";
    public static final String OUTPUT_FILE_Q2_24HOUR        = ".../outputQ2_24Hour.csv";
    public static final String OUTPUT_FILE_Q2_7DAY          = ".../outputQ2_7Day.csv";
    public static final String OUTPUT_FILE_Q2_1MONTH        = ".../outputQ2_1Month.csv";

}
