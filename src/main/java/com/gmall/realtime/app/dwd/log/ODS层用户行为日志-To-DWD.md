## ODS层用户行为日志-To-DWD

#### 数据内容及格式分析

用户行为日志分为：启动日志、页面日志、曝光日志、动作日志、错误日志，启动日志与页面日志互斥，启动日志包含错误日志、页面日志包含曝光日志、动作日志、错误日志。

#### 需求

1. 将ODS层的原始log日志处理成5类数据并保存到对应的kafka主题
2. 用户独立访客明细表需求（该明细表用来统计日活）
3. 用户跳出明细表需求（求用户跳出率）

### 数据格式

页面日志格式

```json
{
    "actions":[
        {
            "action_id":"get_coupon",
            "item":"3",
            "item_type":"coupon_id",
            "ts":1705133388163
        }
    ],
    "common":{
        "ar":"110000",
        "ba":"Redmi",
        "ch":"web",
        "is_new":"1",
        "md":"Redmi k30",
        "mid":"mid_404358",
        "os":"Android 11.0",
        "uid":"61",
        "vc":"v2.1.132"
    },
    "displays":[
        {
            "display_type":"recommend",
            "item":"7",
            "item_type":"sku_id",
            "order":1,
            "pos_id":5
        },
        {
            "display_type":"query",
            "item":"21",
            "item_type":"sku_id",
            "order":2,
            "pos_id":5
        },
        {
            "display_type":"query",
            "item":"24",
            "item_type":"sku_id",
            "order":3,
            "pos_id":3
        },
        {
            "display_type":"query",
            "item":"17",
            "item_type":"sku_id",
            "order":4,
            "pos_id":3
        },
        {
            "display_type":"query",
            "item":"11",
            "item_type":"sku_id",
            "order":5,
            "pos_id":4
        },
        {
            "display_type":"query",
            "item":"7",
            "item_type":"sku_id",
            "order":6,
            "pos_id":2
        },
        {
            "display_type":"promotion",
            "item":"13",
            "item_type":"sku_id",
            "order":7,
            "pos_id":1
        },
        {
            "display_type":"promotion",
            "item":"29",
            "item_type":"sku_id",
            "order":8,
            "pos_id":2
        },
        {
            "display_type":"promotion",
            "item":"31",
            "item_type":"sku_id",
            "order":9,
            "pos_id":2
        },
        {
            "display_type":"promotion",
            "item":"34",
            "item_type":"sku_id",
            "order":10,
            "pos_id":4
        }
    ],
    "page":{
        "during_time":2326,
        "item":"19",
        "item_type":"sku_id",
        "last_page_id":"good_list",
        "page_id":"good_detail",
        "source_type":"promotion"
    },
    "ts":1705133387000
}
```

启动日志格式

```JSON
{
    "common":{
        "ar":"110000",
        "ba":"Redmi",
        "ch":"web",
        "is_new":"1",
        "md":"Redmi k30",
        "mid":"mid_23409",
        "os":"Android 11.0",
        "uid":"294",
        "vc":"v2.1.134"
    },
    "start":{
        "entry":"icon",
        "loading_time":6259,
        "open_ad_id":11,
        "open_ad_ms":5759,
        "open_ad_skip_ms":0
    },
    "ts":1705133383000
}
```

### 需求实现

1. 将ODS层的原始log日志处理成5类数据并保存到对应的kafka主题

   - 消费topic_log主题数据，将脏数据写到测输出流。
   - 首先判断数据中是否存在err，如果存在则写入errTag的侧输出流，remove掉err数据；然后判断是否是start数据，如果是启动日志则直接将剩余数据写入startTag数据；否则说明该条数据是日志数据，则提取common数据以及ts，然后将common、ts数据put进displays数据将displays数据写入displaysTag。将common数据put进actions数据，然后将actions数据写入actionsTag侧输出流，然后remove掉dispalys数据以及actions数据，剩下的数据即为page数据，写入pageTag侧输出流。
   - 将各个侧输出流数据写到对应的kafka主题：dwd_traffic_page_log、dwd_traffic_start_log、dwd_traffic_display_log、dwd_traffic_action_log、dwd_traffic_error_log。
   - 执行操作。

2. 用户独立访客明细表需求（该明细表用来统计日活）

   分析：由于用来统计日活，所以需要对mid进行keyby，然后取出每天第一条访问数据（去重操作）。需要用到flink的状态编程，状态里保存当天的日期，当日期为null或者状态值不等于当前日期时说明，该数据是第一天数据，然后collect写出该数据并更新状态。

   优化：设置状态的TTL，如果状态不设置TTL，对于相隔很久再访问的用户，状态中则一直保存着上一次访问日期。由于统计日活，则设置状态的TTL为1day，更新策略为：OnCreateAndWrite，即当状态更新时TTL也更新，若超过一天状态不更新则状态清空。

   - 消费dwd_traffic_page_log主题数据，将脏数据写到测输出流。
   - 按照mid左keyby分组。
   - 使用filter算子对数据进行过滤，状态里保存日期，当当前数据日期!=状态value或者状态==null时，说明当前数据是当天第一条数据，则返回true，并更新状态，否则返回false。
   - fliter后的DataStream写入dwd_traffic_unique_visitor_detail主题。
   - 执行操作

3. 用户跳出明细表需求（求用户跳出率）

   分析：用户跳出的意思是连续两条页面数据的last_page_id为null，这样可以说明连续两条数据中的第一条数据是跳出数据。对于连续两条last_page_id=null的数据，可以肯定第一条数据一定是跳出数据，但是第二条则不一定，如下图所示。

   思路：对mid进行keyby，然后使用CEP指定过滤规则使用within进行开窗。规则使用严格近邻过滤出连续两条last_page_id=null的数据，然后CEP.pattern(keybyDs, pattern)得到patternDs，然后patternDs.select进行匹配，将超时数据写到侧输出流，最后将selectDs和timeoutDs进行union成DataStream，最后将合并后的数据写到dwd_traffic_user_jump_detail主题。

   - 消费dwd_traffic_page_log主题数据，指定水位线为乱序延迟2s推进，将脏数据写到测输出流。

   - 按照mid进行keyby分组。

   - 指定CEP规则过滤出连续两条last_page_id=null的数据。

   - pattern规则作用到keybyDs，select第一条数据，超时也选择第一条数据。

   - selectDs.union(timeoutDs)。

   - unionDs写入dwd_traffic_user_jump_detail主题。

   - 执行操作。

     

   ![image-20240116112904252](https://raw.githubusercontent.com/LiuSung/Images/main/img/202401161129598.png)

   

   

