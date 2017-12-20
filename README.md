![logo.jpg](http://upload-images.jianshu.io/upload_images/4798589-2bbea3ccdafba60b.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
=========================
一个基于阿里巴[Canal](https://github.com/alibaba/canal)，实时同步mysql数据到Elasticsearch的工具。轻量，易于配置，部署简单，支持数据回滚。
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



