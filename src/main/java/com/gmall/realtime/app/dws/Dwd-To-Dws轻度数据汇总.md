## Dwd-To-Dws轻度数据汇总

### 需求一:关键词需求

#### 方法一：Table Api

```json
{"common":{"ar":"230000","uid":"257","os":"Android 11.0","ch":"xiaomi","is_new":"0","md":"Xiaomi 10 Pro ","mid":"mid_643628","vc":"v2.1.132","ba":"Xiaomi"},"page":{"page_id":"good_list","item":"苹果手机","during_time":15361,"item_type":"keyword","last_page_id":"home"},"ts":1705154404000}
```

1. DDL 方式获取kafka 中dwd_traffic_page_log主题数据，读取page、ts数据创建page_log表，指定事件时间。
2. 过滤出page_log表中的page['item'] 以及ts(where page['last_page_id'] = 'search' and page['item_type'] = 'keyword' and page['item'] is not null)，创建临时视图。
3. 自定切词方法（UDTF）。
4. 使用自定义的UDTF切词方法对item进行切词item->words。
5. 对 word进行分组，开窗口（滚动窗口10s）、聚合。
6. 分组、开窗、聚合函数转换为追加流(KeyWordBean对象)。
7. 将追加流写入Clickhouse的dws_traffic_source_keyword_page_view_window表中。（需要自定义SinkFuction，自定义SinkFuction时用到反射和注解）

#### 方法二： DataStream Api

1. fromSource算子读取dwd_traffic_page_log中的数据，定义水位线。
2. 使用process算子将脏数据写到侧输出流。
3. 使用Fliter算子过滤出满足 page['last_page_id'] = 'search' and page['item_type'] = 'keyword' and page['item'] is not null 的page数据。
4. 使用FlatMap算子对每一条数据中的item进行切词，将切词结果逐个写出。
5. 使用keyby算子进行分组。
6. 使用window算子进行开窗（滑动窗口）。
7. 使用proces算子ProcessWindowFunction进行全量聚合，获取窗口开始结束时间，将窗口开始、结束时间，keyword，count值，'search'、ts写出。
8. 使用map算子将String对象转换成KeyWordBean对象。
9. 使用自定义的SinkFunction将数据写到clickhouse的dws_traffic_source_keyword_page_view_window表中。

#### 目标效果

![image-20240123230917391](https://raw.githubusercontent.com/LiuSung/Images/main/img/202401232309161.png)