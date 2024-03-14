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

**需要先开启BaseLogApp，再开启dwstrafficsourcekeywordpageviewwindow，因为该任务读取BaseLogApp任务写入dwd_traffic_page_log主题的数据**

![image-20240313220213128](https://raw.githubusercontent.com/LiuSung/Images/main/img/202403132202073.png)

#### 查看Clickhouse表结果

![image-20240123230917391](https://raw.githubusercontent.com/LiuSung/Images/main/img/202401232309161.png)

### 需求二：版本渠道地区访客类别页面浏览需求

以版本、渠道、地区、访客维度作为key做分区，求独立访客数、用户跳出数、页面浏览数、浏览时间、会话数，并写到ClickHouse

#### DataStream Api

1. fromSource 算子读取dwd_traffic_unique_visitor_detail、dwd_traffic_user_jump_detail、dwd_traffic_page_log主题的数据，将String对象转换成TrafficPageViewBean(包含窗口开始时间、截至时间、版本、渠道、地区、访客类型、独立访客数、用户跳出数、页面浏览数、页面浏览时间、会话数、ts数据产生时间戳)对象。
2. 三条数据流做union。
3. 提出事件事件生成WarterMark（水位线延迟时间设置为14s）。
4. 按照版本、渠道、地区、访客做keyby。
5. 使用reduce进行增量聚合（采用增量与全量结合的api，目的是利用全量的api获取窗口的开始与结束时间）
6. 将TrafficPageViewBean数据写入ClickHouse。
7. 启动任务执行。

**需要先开启BaseLogApp、dwdtrafficuserjumpdetail、dwdtrafficuniquevisitordetail任务**

![image-20240313221523545](https://raw.githubusercontent.com/LiuSung/Images/main/img/202403132215139.png)

#### 查看Clickhouse表结果

![image-20240313224320794](https://raw.githubusercontent.com/LiuSung/Images/main/img/202403132243815.png)

### 需求三：页面浏览个窗口汇总

从 Kafka 页面日志主题读取数据，统计当日的首页和商品详情页独立访客数。

#### DataStream Api

1. fromSource读取dwd_traffic_page_log主题数据，定义水位线，水位线乱序2s。
2. 过滤出page_id为home和good_detail的数据。
3. 按照mid进行keyby。
4. 使用FlatMap算子将数据转换成TrafficHomeDetailPageViewBean对象，并做使用状态变成进行去重，值状态保存home或者good_detail数据的日期，状态ttl为1day，ttl更新策略为写更新。如果状态为null，说明是当日第一次访问则home_un_ct=1,good_detail_unct=1并更新状态。如果状态不为null并且状态值不等于当前这条数据的日期则home_un_ct=1,good_detail_unct=1并更新状态。
5. 进行全窗口聚合
6. 使用增量聚合和全量聚合的方式计算（全量聚合的目的是获取窗口时间）
7. 将数据写入clickhouse
8. 执行

![image-20240313221335508](https://raw.githubusercontent.com/LiuSung/Images/main/img/202403132213525.png)

![image-20240228221909959](https://raw.githubusercontent.com/LiuSung/Images/main/img/202402282219284.png)



### 需求四：用户登录各窗口汇总表

从 Kafka 页面日志主题读取数据，统计七日回流用户和当日独立用户数。

1. fromSource读取dwd_traffic_page_log主题数据，定义水位线，水位线乱序2s。
2. FlatMap算子过滤登录数据(游客登录：user_id!=null && last_page_id=null 直接登录：last_page_id=login)，并实现string到Json类型的数据转换。
3. 按照user_id进行分组beyby
4. 使用flatmap算子实现JSON到UserLoginBean类型的转换，并用使用状态(保存日期，ttl为1天，写更新)编程统计当前数据是否为当天登录独立用户和七日回流用户（如果状态为null 独立用户数为1，状态!=null，如果当前条数据日期!=状态值，独立用户数为1，并且转换成ts之后只差大于8天，则七日回流用户=1）
5. 开窗全窗口，使用增量和全量结合的方式进行聚合
6. 将数据写如clickhouse，执行。

![image-20240313221358818](https://raw.githubusercontent.com/LiuSung/Images/main/img/202403132214181.png)

![image-20240228224250913](https://raw.githubusercontent.com/LiuSung/Images/main/img/202402282242094.png)

### 需求五：用户注册各窗口汇总表

从 DWD 层用户注册表中读取数据，统计各窗口注册用户数。

1. fromSource算子设置dwd_user_register主题数据，设置乱序时间为2s。
2. string类型数据转换成UserRegisterBean数据。
3. 全窗口开窗聚合
4. 写入clickhouse执行

任务开启顺序：dwduserregister—>dwsuseruserregisterwindow

![image-20240313224641103](https://raw.githubusercontent.com/LiuSung/Images/main/img/202403132246165.png)

![image-20240229101654217](https://raw.githubusercontent.com/LiuSung/Images/main/img/202402291016673.png)

### 需求六：加购各窗口汇总表

从 Kafka 读取用户加购明细数据，统计每日各窗口加购独立用户数。

1. fromSource读取dwd_trade_cart_add主题数据，设置水位线乱序2s。如果operate_time!=null 则根据operate_time时间设置水位线，如果operate_time==null 则按照create_time设置水位线。
2. 按照user_id进行keyby。
3. flatmap算子使用值状态编程去重，状态里保存日期，去重策略同上并实现类型转换，将JSON类型转换为CartAddUuBean类型。
4. 全窗口开窗，增量全量结合的方式聚合
5. 写入clickhouse，执行程序。

任务开启顺序：dwdtradecartadd—>dwstradecartadduuwindow

![image-20240313225146791](https://raw.githubusercontent.com/LiuSung/Images/main/img/202403132251035.png)

![image-20240313225355691](https://raw.githubusercontent.com/LiuSung/Images/main/img/202403132253315.png)

### 需求七：支付各窗口汇总表

从 Kafka 读取交易域支付成功主题数据，统计当日支付成功独立用户数和首次支付成功用户数。

支付成功主题数据是由：支付详情表、订单详情表、订单明细表、活动表详情表、优惠券详情表，5张表join得到的数据，其中活动表详情表、优惠券详情表在join时使用left join，所以下游数据中存在撤回流数据，如果要计算订单等数据需要先对支付成功主题数据进行去重。

1. fromSource读取dwd_trade_pay_detail主题数据
2. flatmap算子过滤null数据并将string转换成JSON
3. 状态变成+定时器功能进行去重，如果状态==null，JSON更新到状态，注册5s定时器，如果状态!=null，判断当前JOSN与状态中JSON的生成时间哪个大，将大的更新到状态中。
4. 按照user_id进行keyby
5. flatmap去重（去重方法同上）并实现JSON到TradePaymentWindowBean的转换。
6. 全窗口开窗，增量全量集合的方式进行聚合
7. 写入clickhouse，执行程序。

任务开启顺序：dwdtradeorderpreprocess（订单预处理任务）->dwdtradeorderdetail（订单明细任务）->dwdtradepaydetail（支付明细表）->dwstradepaymentsucwindow（支付个窗口汇总任务）

![image-20240314093216670](https://raw.githubusercontent.com/LiuSung/Images/main/img/202403140932593.png)

![image-20240229105750139](https://raw.githubusercontent.com/LiuSung/Images/main/img/202402291057669.png)

### 需求八：下单各窗口汇总表

从 Kafka 订单明细主题读取数据，对数据去重，统计当日下单独立用户数和新增下单用户数，封装为实体类，写入 ClickHouse。

下单主题数据是由：订单详情表、订单明细表、活动表详情表、优惠券详情表四张表join来的，同样也存在left join。因此也需要进行去重。

该需求和需求六基本相同。

任务开启顺序：dwdtradeorderpreprocess（订单预处理任务）->dwdtradeorderdetail（订单明细任务）—>dwstradeorderwindow(下单各窗口汇总表)

![image-20240314100030613](https://raw.githubusercontent.com/LiuSung/Images/main/img/202403141000811.png)

### 需求九：用户spu粒度下单汇总表

从 Kafka 订单明细主题读取数据，过滤 null 数据并按照唯一键对数据去重，关联维度信息，按照维度分组，统计各维度各窗口的订单数和订单金额，将数据写入 ClickHouse 交易域品牌-品类-用户-SPU粒度下单各窗口汇总表。

任务：

1. 去重（上游使用了left join 存在撤回流，去重方案使用状态编程+定时器）
2. map方法将JSONObject转换成JavaBean对象
3. 关联HBase中的维表，补充spu_id，tm_id，category3_id
4. 提取事件事件
5. 分组开窗(user_id,spu_id,tm_id,category3_id)、聚合
6. 关联维表补充spu_nam，tm_name，category3_name，category2_name，category1_name
7. 将数据写入clikhouse执行

关联维度表优化：

1. 旁路缓存：参考HBase的读缓存模型，使用redis进行数据缓存，设计维度表的读缓存和写缓存。在读取维度数据时首先根据tablename+id作为key去redis中查找，如果redis中再去Hbase中查找然后将找到的数据缓存到redis。在ods-to-dim层如果获取到的某条数据的type=update说明是更新维度信息的，那么在更新前将redis中的缓存先删掉。
2. 异步IO：虽然使用旁路缓存优化了数据读取速度，但是补充维度信息的map方法是同步方法，一条数据只能等待其返回结果下一条数据才能查询，这样大部分时间浪费在了等待返回结果上。因此使用flink提供的异步IO接口+线程池(双重校验的单例设计模式)+连接池实现异步IO，这样一个并行度可以同时处理多条数据。

知识点：

1. 对于泛型对象如果要获取其属性值有两种方法：
   - 反射：通过泛型对象的class对象获取属性值。
   - 抽象方法(一定是public)与抽象类：在new 一个抽象类时需要重写抽象方法，如果该抽象方法是一个泛型方法那么在重写方法时传入的类型九确定了，那么可以在重写的抽象方法里实现获取属性值。

任务开启顺序：dwdtradeorderpreprocess（订单预处理任务）->dwdtradeorderdetail（订单明细任务）—>dwstradeuserspuorderwindow（spu粒度下单汇总窗口）

此外还需要开启redis服务

![image-20240314140627187](https://raw.githubusercontent.com/LiuSung/Images/main/img/202403141406887.png)

![image-20240314100547981](https://raw.githubusercontent.com/LiuSung/Images/main/img/202403141005088.png)

### 需求十：省份粒度下单汇总表

从 Kafka 读取订单明细数据，过滤 null 数据并按照唯一键对数据去重，统计各省份各窗口订单数和订单金额，将数据写入 ClickHouse 交易域省份粒度下单各窗口汇总表。

1. 从 Kafka订单明细主题读取数据

2. 转换数据结构

3. 按照唯一键去重（先keyby，然后状态编程+定时器）

4. 转换数据结构（JSONObject 转换为实体类 TradeProvinceOrderWindow）
5. 设置水位线
6. 按照省份分组，开窗聚合
7. 查询维表关联省份信息补全字段

8. 写出到 ClickHouse，执行

wdtradeorderpreprocess（订单预处理任务）->dwdtradeorderdetail（订单明细任务）—>dwstradeprovinceorderwindow（省份粒度下单汇总窗口）

![image-20240314100521322](https://raw.githubusercontent.com/LiuSung/Images/main/img/202403141005021.png)

![image-20240314101913426](https://raw.githubusercontent.com/LiuSung/Images/main/img/202403141019712.png)

Redis中缓存数据：

![image-20240314123607567](https://raw.githubusercontent.com/LiuSung/Images/main/img/202403141236001.png)