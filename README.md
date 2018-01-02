# 基于linkin的databus,改造为maven项目,并添加了pg的支持插件,对逻辑做了略微改造

@(示例笔记本)[马克飞象|帮助|Markdown]



## 改造点如下:
      > 1.原版设计的relay收到的scn小于当前最小scn和prevScn时,是会返回scnNotFoundException(安全稳定考虑),
      现添加了client的配置项skipWTE,可以直接从当前relay的最小事件开始消费,用于bootstrap的同步
      > 2.优化了读取文件的代码

## 源码解读文档地址:
   
## pg接入databus架构图:
   ![]()
