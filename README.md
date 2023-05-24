# README

## Java环境配置

- 确保自己的电脑环境已经按照JDK，貌似openJDK会有bug，所以最好安装JDK

- 安装maven，并创建空项目
- 将pom.xml内容复制到maven的pom.xml
- 可以直接将main文件夹直接放到maven的src目录下，也可以在Java同一目录下创建resource文件夹，将log4j.properties放进去，然后.java一个一个复制

## zookeeper环境配置

具体配置过程可以参考网络上的教程

启动zookeeper集群后，需要修改各个Java文件的连接字段

`private String connectionString = "192.168.10.102:2181";`，后面的IP地址根据zookeeper集群的连接地址确定

## MySQL数据库配置

本项目采用的数据库是MySQL数据库，故也要修改涉及到数据库连接的Java文件，只需要修改连接的数据库名，用户名和密码即可

