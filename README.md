# 基于linkin的databus,改造为maven项目,并添加了pg的支持插件,对逻辑做了略微改造

@(示例笔记本)[马克飞象|帮助|Markdown]

## 基本概念介绍和官方链接
   > [官方链接][https://github.com/linkedin/databus]
   > [一篇极好的讲解][http://tech.lede.com/2017/05/24/rd/server/databus/]

## 改造点如下:
      > 1.原版设计的relay收到的scn小于当前最小scn和prevScn时,是会返回scnNotFoundException(安全稳定考虑),
      现添加了client的配置项skipWTE,可以直接从当前relay的最小事件开始消费,用于bootstrap的同步
      > 2.优化了读取文件的代码

## 源码解读文档地址:
   
## pg接入databus架构图:
   ![](https://raw.githubusercontent.com/cj121992/databus-maven/master/sources/test.png)  

## pg接入databus步骤:
   - **1.数据库logical_decoding功能开启**
           <br> (1) pg 9.4以上
           <br> (2) 将 wal_level这个配置参数设置为logical，并保证max_replication_slots至少为1
           <br> (3) 创建复制槽，输出格式要求为test_decoding
   - **2.relay配置槽名和可使用帐号，启动relay**
   - **3.配置bootstrap所使用的mysql数据库（建议单独建库），用于持久化事件
     <br>bootstrap有两个组件，producer 负责拉取变化，写入mysql，servert提供http接口返回client的查询。**
   - **4.根据实际需求写所需的client**

## 流程说明:
   - 1.正常工做模块，按启动步骤搭建即可。
   - 2.relay宕机，重启即可。由于pg是基于复制槽做的，宕机并不会错过消费进度，但是重启之后relay的之前事件丢失，最小事件会变为最新的一个变化流的scn。
   <br>如果有消费进程没有即时消费之前的事件导致进度落后，可以启动bootstrap的server组件，设置client消费对应的server组件，消费之前未消费的事件，
   <br>当scn消费到最新时，会自动切换为relay消费。
   - 3.relay的内存设置有限，需要追溯更早的事件，同理，也是通过读取boostrap持久化到mysql的数据去追溯。
   
