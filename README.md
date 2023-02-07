# happydb

<p align='center'>
<img src="https://img.shields.io/badge/build-passing-brightgreen.svg">
<img src="https://img.shields.io/badge/platform-%20WINDOWS | MAC | LINUX%20-ff69b4.svg">
<img src="https://img.shields.io/badge/language-JAVA-orange.svg">
<img src="https://github.com/ShrBox/ACGPro/workflows/Java%20CI%20with%20Gradle/badge.svg">
<img src="https://img.shields.io/badge/Author-Happysnaker-green.svg">
<img src="https://img.shields.io/badge/Name-happydb-yellow.svg">
</p>

## 介绍

*happydb* 是一款基于 Java 语言实现的简易关系型数据库，取名为 *happydb* 并不意味着它是个 "搞笑的数据库"，而是希望在做此项目时能够开开心心地一边学习数据库知识、一边完成项目。  

*happydb* 在某些功能的实现上借鉴了 [MIT-6.830](https://github.com/MIT-DB-Class/simple-db-hw-2021)，如果彻底理解了此项目，相信你也可以轻而易举地完成 MIT-6.830 实验。

## 特点

- 支持基本的增删改查语句，其中查询语句支持如过滤、分组、聚合、连接、排序等表达式。
- 基于代价的查询优化器决定最佳过滤方案与查询计划，平均性能提升两倍以上。
- 基于蟹行协议实现可供并发访问的 B+ 树索引，能抗住十万级并发流量。
- 基于 `redo log` 和 `undo log` 实现了 `STEAL/NO-FORCE` 模式下的回滚与恢复，保证了事务的原子性与持久性。
- 实现了基于 `STEAL/NO-FORCE` 模式下的检查点机制，以加快崩溃恢复例程。
- 实现了行级别的二阶段锁定，并支持死锁检测。
- 基于 `undo log` 引用链实现了 MVCC 功能，保证了事务的隔离性。
- 基于 **Raft 算法** 实现了主从复制和故障恢复机制。
- 基于 **Netty** 实现了客户端与数据库之间高效的通信。

## 缺陷

作为一个学习项目，由于精力、时间等因素，本项目并没有那么健全，存在一些缺陷，这些缺陷是可以忍受的。

- 没有实现对索引页的 redo 和 undo，这导致修改索引列非常麻烦，引入了很多额外的工作保障索引页与数据页的一致性，我们甚至不允许对索引列的 `UPDATE`。
- 没有实现 `NextKey Locking` 算法，不支持 `FOR UPDATE` 表达式。
- 没有引入 `Double Write` 机制，我们总是假定磁盘写入是原子的，即使写入数据超过了一个扇区的大小。
- 在高并发情况下，B+ 树存储字符串类型数据存在问题，但存储数值类型数据没有问题，暂时没有排查到原因。
- Raft 算法代码缺少足够的单元测试、Mock 测试以及集成测试，这不是一个简单的工作。
- 一些复杂的语句暂不支持。

2023/2/7 更新，在实现 undo log page 之后，任何一条 UPDATE 语句都必须同时创建 undo log，并且要为 undo log 同时创建 redo log，这会导致 大量的 IO，导致原先的插入有 10ms 增加到 500ms，原先能支持上万并发插入现在直接寄，大寄特寄，如果不实现 STEAL 模式的话理论上不需要 undo log page 的，有空会继续进行优化的。

## 项目演示
项目中有一个 mp4 文件，为开发时 [录屏文件](./test01.mp4)，可查阅大部分功能演示。

这个录屏还演示了查询优化将一个连接查询由 12s 优化到 5s，以及索引性能测试，等值查询耗时为 0ms，在这个视频中，我们看到并发创建 1w 条行记录也仅仅耗时 50s，**并发插入平均耗时 5ms，这是相当高的性能，实际上能够抗住上百万的并发量。**

## 文档指引

可参考 [HappyDB 方案](./docs/abc.md)，更详细文档待编写。

## 致谢
- [MIT-6.830](https://github.com/MIT-DB-Class/simple-db-hw-2021)