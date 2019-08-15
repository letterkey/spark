## 源码阅读
. spark-submit shell <br>
. 调用spark 源码中的main class：org.apache.spark.launcher.Main  function main 主要做提交的参数 java参数等的整理<br>
. 调用SparkSubmit class:org.apache.spark.deploy.SparkSubmit function doSubmit 之后做 cluster deploy的封装和路由 并以RPC方式请求cluster manager <br>
. 对spark standlone，上述会请求Master class： org.apache.spark.deploy.master.Master function receiveAndReply  分配资源和启动driver、executor <br>
. driver的启动将会运行 用户的application，即用户编写的spark程序，这个程序封装为 org.apache.spark.deploy.worker.DriverWrapper class 所以实际启动的是此类的main函数，main函数中再启动用户的app main <br>

. 上述过程，此时正式启动用户编写的app的main函数，做spark context的初始化：app注册 DAGSchedule TaskSchedule SecurityManager 等初始化 <br>

# 有两类
    * shuffle map stage
    * result stage
    
[stage 切分](https://www.cnblogs.com/xing901022/p/6674966.html)