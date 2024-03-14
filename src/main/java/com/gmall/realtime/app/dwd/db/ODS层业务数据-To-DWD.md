## ODS层业务数据-To-DWD

dwd层业务数据包含交易域（6张事务事实表）、工具域（3张事务事实表）、互动域（2张事务事实表）、用户区（1张事实表）

知识：Flink SQL：LookUp Join、Join、Left Join

### 交易域业务过程

![image-20240121112139659](https://raw.githubusercontent.com/LiuSung/Images/main/img/202401211121212.png)

#### 加购事实表

1. 获取kafka中ods_topic_db主题数据
2. 筛选出加购数据封装成表
3. 建立Mysql Look Up字典表
4. 关联加购表和字典表获取维度退化之后的表（dic_code与source_type关联）
5. 根据关联后的表字段创建加购事实表DDL
6. 写入数据主![image-20240313195650288](https://raw.githubusercontent.com/LiuSung/Images/main/img/202403132001281.png)

消费dwd_trade_cart_add数据查看是否写入该topic

![image-20240313200017929](https://raw.githubusercontent.com/LiuSung/Images/main/img/202403132001760.png)

#### 订单预处理表

​	订单明细表和取消订单明细表的数据来源、表结构都相同，差别只在业务过程和过滤条件，为了减少重复计算，将两张表公共的关联

过程提取出来，形成订单预处理表。关联订**单明细表、订单表、订单明细活动关联表、订单明细优惠券关联表四张事实业务表和字典表（维度业务表）形成订单预处理表**，写入 Kafka 对应主题。

​	注意：主表与订单明细活动关联表、订单明细优惠券关联表join时使用left join，由于left join有撤回操作写入Kafka主题时创建的表需要使用upsert-kafka。状态TTL设置为5s。

![image-20240121114138978](https://raw.githubusercontent.com/LiuSung/Images/main/img/202401211141982.png)

![image-20240313200350848](https://raw.githubusercontent.com/LiuSung/Images/main/img/202403132003993.png)

读取dwd_trade_order_pre_process数据，并观察撤回流(先写如null再写入数据)

![image-20240313200647386](https://raw.githubusercontent.com/LiuSung/Images/main/img/202403132006517.png)

#### 下单事务事实表

1. 读取Kafka订单预处理主题数据
2. 使用Table Api将流转换成表
3. 过滤出Insert数据即为下单事务
4. 根据过滤出的字段创建下单事务事实表DDL
5. 数据写入Kafka下单事务事实表主题（dwd_trade_order_detail）

![image-20240313200824898](https://raw.githubusercontent.com/LiuSung/Images/main/img/202403132008253.png)

![image-20240313200930811](https://raw.githubusercontent.com/LiuSung/Images/main/img/202403132009165.png)

#### 取消订单事实事实表

1. 读取Kafka订单预处理主题数据
2. 使用Table Api将流转换成表
3. 过滤出update数据即（order_status = '1003' and old['order_status'] is not null）
4. 根据过滤出的字段创建取消订单事务事实表DDL
5. 数据写入Kafka取消订单事务事实表主题（dwd_trade_order_detail）

![image-20240313201538734](https://raw.githubusercontent.com/LiuSung/Images/main/img/202403132015135.png)

![image-20240313201648915](https://raw.githubusercontent.com/LiuSung/Images/main/img/202403132016172.png)

#### 支付成功事务事实表

​	ods_topic_db主题筛选支付成功数据、从dwd_trade_order_detail主题中读取订单事实数据、MySQL-LookUp字典表，关联三张表形成支付成功宽表，写入 Kafka 支付成功主题。

​	注意：下单到支付一般设置15min间隔，即从下单之后要在15min之后支付，再考虑数据乱序的可能，设置flink Join 时状态TTL为15min+5s。

1. 设置环境，开启checkpoint，设置状态TTL
2. 读取dwd_trade_order_detail主题数据，将流转换成表
3. 读取topic_db主题数据，流转换成表并过滤出支付成功数据
4. 读取Mysql-LookUp字典表数据
5. 三张表Join形成宽表
6. 创建支付成功事实表写入kafka对应主题（dwd_trade_pay_detail_suc）

![image-20240313201909318](https://raw.githubusercontent.com/LiuSung/Images/main/img/202403132019694.png)

![image-20240313202153833](https://raw.githubusercontent.com/LiuSung/Images/main/img/202403132021905.png)

#### 退单事务事实表

​	ods_topic_db主题数据中过滤出退单表数据以及符合条件的订单表数据，关联Mysql-LookUp字典表（订单表中获取pronvice_id，字典表获取退款类型名称以及退款原因类型名称）

​	注意：状态TTL设置为5s。

1. 设置环境，开启checkpoint，设置状态TTL
2. 读取topic_db主题数据，流转表，过滤出退单表数据；过滤出(type = 'update' and old['order_status'] is not null and data['order_status'] = 1005)的订单表数据
3. 获取Mysql-LookUp 字典表
4. 三表join
5. 创建退单事务事实表DDL
6. 将数据写入对应的Kafka主题（dwd_trade_order_refund）

![image-20240313202805006](C:/Users/xuan/AppData/Roaming/Typora/typora-user-images/image-20240313202805006.png)

![image-20240313202944163](https://raw.githubusercontent.com/LiuSung/Images/main/img/202403132029474.png)

### 工具域业务过程

![image-20240121155642298](https://raw.githubusercontent.com/LiuSung/Images/main/img/202401211556712.png)

#### 优惠券领取事务事实表

获取ods_topic_db主题数据，过滤出type='insert'的coupon_use表数据。Table Api使用的Kafka连接器为Upsert-Kafka。

![image-20240313211419587](https://raw.githubusercontent.com/LiuSung/Images/main/img/202403132114780.png)

![image-20240313213617037](https://raw.githubusercontent.com/LiuSung/Images/main/img/202403132136905.png)

#### 优惠券使用(下单)事务事实表

​	获取ods_topic_db主题数据，过滤出(type='update' and data['coupon_status'] = '1402' and old['coupon_status'] = '1402']的coupon_use表数据。Table Api使用的Kafka连接器为Upsert-Kafka。

![image-20240313211604411](https://raw.githubusercontent.com/LiuSung/Images/main/img/202403132116880.png)

![image-20240313213657089](https://raw.githubusercontent.com/LiuSung/Images/main/img/202403132136338.png)

#### 优惠券使用(支付)事务事实表

​	获取ods_topic_db主题数据，过滤出(type='update' and data['use_time'] is not null]的coupon_use表数据。Table Api使用的Kafka连接器为Upsert-Kafka。

![image-20240313211631100](https://raw.githubusercontent.com/LiuSung/Images/main/img/202403132116092.png)

![image-20240313213810159](https://raw.githubusercontent.com/LiuSung/Images/main/img/202403132138519.png)

### 互动域业务过程

![image-20240121111148524](https://raw.githubusercontent.com/LiuSung/Images/main/img/202401211111594.png)

#### 收藏商品事实事实表

​	获取ods_topic_db主题数据，过滤出type='insert'的收藏表数据。

![image-20240313214928835](https://raw.githubusercontent.com/LiuSung/Images/main/img/202403132149201.png)

![image-20240313215533850](https://raw.githubusercontent.com/LiuSung/Images/main/img/202403132155979.png)

#### 评价事务事实表

​	获取ods_topic_db主题数据，过滤出type='insert'的评价表数据，读取Mysql-LookUp表字典数据（获取评价类型名称：好评、中评、差评），TTL设置为5s。

![image-20240313214908569](https://raw.githubusercontent.com/LiuSung/Images/main/img/202403132149730.png)

![image-20240313215707734](https://raw.githubusercontent.com/LiuSung/Images/main/img/202403132157732.png)

### 用户域业务过程

![image-20240121111307937](https://raw.githubusercontent.com/LiuSung/Images/main/img/202401211113620.png)

​	获取ods_topic_db主题数据，过滤出type='insert'的用户表数据。

![image-20240313215230634](https://raw.githubusercontent.com/LiuSung/Images/main/img/202403132152339.png)

![image-20240313215745449](https://raw.githubusercontent.com/LiuSung/Images/main/img/202403132157726.png)
