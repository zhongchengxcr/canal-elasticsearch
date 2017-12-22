![logo.jpg](http://upload-images.jianshu.io/upload_images/4798589-0177ebbf0e0e007e.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
=========================
一个基于阿里巴巴[Canal](https://github.com/alibaba/canal)，实时同步mysql数据到Elasticsearch的工具。轻量，易于配置，部署简单，支持数据回滚。
使用 Totoro 可以帮助你轻松的将mysql的数据实时同步到Elasticsearch。Totoro是基于阿里巴巴的canal，是数据库级别的
监听，对原有项目没有任何侵入，所以你无需更改项目中的任何代码就可以实现实时的数据同步。


[![AppVeyor](https://img.shields.io/appveyor/ci/gruntjs/grunt.svg)]()


[Elasticsearch](https://www.elastic.co/cn/)是一个分布式搜索服务，提供Restful API，底层基于Lucene，
采用多shard的方式保证数据安全，并且提供自动resharding的功能，github等大型的站点也采用Elasticsearch作为其搜索服务。

对于Elasticsearch，如果要在项目中使用，第一个要解决的问题就是，如何将数据库中的数据同步到Elasticsearch，下面常见的几种中方案

* 修改代码，修改插入数据库的代码，同时插入Elasticsearch 
* 修改代码，修改插入数据库的代码，同时放入消息队列，在消息队列的另一端进行插入Elasticsearch 
* 使用 Elasticsearch技术栈的[Logstash](https://www.elastic.co/cn/products/logstash)，编写sql语句定时执行搜集数据，并插入到Elasticsearch 

在上诉方案中，一三都有明显缺陷，第二种方案是目前采用最多的，但还是会对原有项目代码有侵入，并且要引入消息队列。
如果你不想引入消息队列并且想获得一个开箱即用的同步中间件，那么Totoro会令你非常满意
。

Totoro的方案是，基于阿里巴巴开源数据库中间件canal，监听mysql数据库，并且在mysql数据库数据发生变化的时候
发送消息给Totoro，Totoro会将数据同步到Elasticsearch，如果消费出错支持回滚，保证数据被正确消费。

![](http://upload-images.jianshu.io/upload_images/4798589-78d5777d4b1128e6.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


工作原理
======================

totoro主要分为4个模块: select、Transformation、consumer、channel
* select 负责从canal中拉取数据
* Transformation 负责将select生产的数据，进行过滤、处理、转换
* consumer 负责消费数据，在这里就是将数据同步到elasticsearch
* channel 是以上3个模块链接者，也是数据在totoro中的容器。select将拉取的数据放入channel，Transformation监听到select放入的数据，进行处理，处理完再放回channel，等待consumer去消费
![](http://upload-images.jianshu.io/upload_images/4798589-b373bb768f382564.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
select、Transformation、consumer分别是3个任务，并行执行。为保证数据消费顺序与ack（每次消费一条数据要向canal进行ack）顺序，
其中select task与consumer task 都分别只有一条线程。而Transformation task具有多条线程。


QuickStart
======================
1.安装Canal

有关于canal的安装，请参见canal的文档，里面有很详细说明 [Canal QuickStart](https://github.com/alibaba/canal/wiki/QuickStart)


2.安装Totoro
```
git clone git@github.com:zhongchengxcr/canal-elasticsearch.git
cd canal-elasticsearch
mvn clean compile package -P dev -Dmaven.test.skip=true

```
编译完成后，进入/target/totoro目录，可以看到如下结构：
```$xslt
-rw-r--r--   1 zhongcheng  staff  2214 12 21 17:08 README.md
drwxr-xr-x   5 zhongcheng  staff   160 12 21 17:22 bin
drwxr-xr-x   5 zhongcheng  staff   160 12 21 17:08 conf
drwxr-xr-x  73 zhongcheng  staff  2336 12 21 17:08 lib
drwxr-xr-x   4 zhongcheng  staff   128 12 21 17:08 logs
```

3.配置修改
```$xslt
vi conf/canal-es.properties

###一下配置需要根据自己的业务进行修改

# ----------------------- canal 相关配置       -----------------------------
#canal的实例名字
totoro.canal.destination=totoro
#cananl 服务端的模式，单机：single ，集群：cluster/
totoro.canal.mode=sign
#canal 地址，包括端口号
totoro.canal.address=127.0.0.1:11111
#过滤表达式
totoro.canal.filter.patten=
#如果canal模式是集群的话，则需要填写zk地址
totoro.canal.zk.address=
totoro.canal.username=
totoro.canal.password=

#此项必须配置，不配置会导致 totoro 启动不了， totoro 只会处理在此配置的表
#没有在此配置的表，totoro 将会忽略，不会进行同步，格式 "database.table.id" 多个使用 "，"分割
#database代表数据库，table 代表 数据库中的表，id代表 table中的 id
#totoro 会默认将 database 作为 es中的index，table作为es中的type ，使用db中的id作为es的id
totoro.canal.table.accept=demo.cc.id


# ----------------------- elasticsearch 相关配置 -----------------------------
totoro.es.address=127.0.0.1:9300
totoro.es.cluster.name=my-elasticsearch
totoro.es.username=
totoro.es.password=
# ----------------------- totoro 相关配置         ----------------------------
#处理信息转换的线程数量 默认 3个 ， 不要配置太大，2-4 之间吧，取决于业务情况，太大并不会增加性能，反而会增加上下文切换的开销
totoro.cannal.trans.thread.nums=3

```
4.准备启动

```$xslt
cd ../bin
./startup.sh


 _____       _
|_   _|___  | |_  ___   _ __  ___  
  | | / _ \ | __|/ _ \ | '__|/ _ \ 
  | || (_) || |_| (_) || |  | (_) |
  |_| \___/  \__|\___/ |_|   \___/
[Totoro 1.0-SNAPSHOT，Build 2017/12/20，Author:zhongcheng_m@yeah.net]

cd to /Users/zhongcheng/IdeaProjects/canal-elasticsearch/target/totoro/bin for workaround relative path
LOG CONFIGURATION : /Users/zhongcheng/IdeaProjects/canal-elasticsearch/target/totoro/bin/../conf/logback.xml
sync conf : /Users/zhongcheng/IdeaProjects/canal-elasticsearch/target/totoro/bin/../conf/canal-es.properties
cd to /Users/zhongcheng/IdeaProjects/canal-elasticsearch/target/totoro/bin for continue

```

5.查看日志

```$xslt
cd ../logs
tail -100f totoro.log

[2017-12-21 17:58:27.699] [INFO] [main] [c.t.c.e.s.s.canal.CanalEmbedSelector] --- TotoroSelector init start  ， conf :CanalConf{mode=SIGN， destination='totoro'， filterPatten=''， address='127.0.0.1:11111'， zkAddress=''， userName=''， accept='demo.cc.id'}
[2017-12-21 17:58:27.719] [INFO] [main] [c.t.c.e.s.s.canal.CanalEmbedSelector] --- TotoroSelector init complete .......
[2017-12-21 17:58:27.737] [INFO] [main] [c.t.c.e.select.selector.SelectorTask] --- Selector task init .......
[2017-12-21 17:58:27.737] [INFO] [main] [c.t.c.e.select.selector.SelectorTask] --- Selector task complete .......
[2017-12-21 17:58:27.748] [INFO] [main] [c.t.c.e.transform.MessageFilterChain] --- TableFilter has benn registered to message filter chain 
[2017-12-21 17:58:27.748] [INFO] [main] [c.t.c.e.transform.MessageFilterChain] --- SimpleMessageFilter has benn registered to message filter chain 
[2017-12-21 17:58:27.749] [INFO] [main] [c.t.c.es.transform.SimpleEsAdapter] --- Add accept :demo.cc.id
[2017-12-21 17:58:27.750] [INFO] [main] [c.t.canal.es.transform.TransFormTask] --- TransFormTask init  start .......
[2017-12-21 17:58:27.758] [INFO] [main] [c.t.canal.es.transform.TransFormTask] --- TransFormTask init complete .......
[2017-12-21 17:58:30.571] [INFO] [main] [c.t.c.e.c.e.i.ElasticsearchServiceImpl] --- Complete the connection to elasticsearch
[2017-12-21 17:58:30.572] [INFO] [main] [c.t.canal.es.consum.es.ConsumerTask] --- Consumer task init start .......
[2017-12-21 17:58:30.573] [INFO] [main] [c.t.canal.es.consum.es.ConsumerTask] --- Consumer task init complete.......
[2017-12-21 17:58:30.573] [INFO] [main] [com.totoro.canal.es.TotoroLauncher] --- Totoro init complete .......
[2017-12-21 17:58:30.574] [INFO] [taskName = TransFormTask] [c.t.canal.es.transform.TransFormTask] --- TransFormTask start .......
[2017-12-21 17:58:30.574] [INFO] [taskName = ConsumerTask] [c.t.canal.es.consum.es.ConsumerTask] --- ConsumerTask start .......
[2017-12-21 17:58:30.656] [INFO] [taskName = SelectorTask] [c.t.c.e.select.selector.SelectorTask] --- Selector task start .......

```

6.关闭
```$xslt
cd ../bin
./stop.sh
```

存在的问题
======================
* 内存抖动与GC时间过高，20万条insert数据，利用 jconsole 观察 ， canal 的内存基本稳定在500m以内
totoro的内存一直在 1G 上下浮动， canal的总垃圾回收时间在 0.14秒左右 ，totorp的垃圾回收总时间在 1.73秒(可能是我创建的对象过多)，
(正在你努力优化，欢迎各为朋友赐教)，以下 图1 为totoro，图2 为canal.

![](http://upload-images.jianshu.io/upload_images/4798589-3e512380b9d15f35.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


![](http://upload-images.jianshu.io/upload_images/4798589-3511591fea0464f4.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240) 

* Elasticsearch 客户端 transport 的 cpu与内存消耗比较多，经测试 发现 transport 启动了 20+ 线程

* 数据转换的灵活性不够

* 单一节点，不支持多节点部署


计划
======================
* 按照目前发现的问题，一一解决
* 优先处理性能问题，欢迎提交 pull request

欢迎有想法的朋友一起参与，讨论
QQ群:688734361
![](http://upload-images.jianshu.io/upload_images/4798589-a34789352b17055f.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
