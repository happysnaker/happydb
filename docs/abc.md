# HappyDB

- 支持基本的查询语句如过滤、分组、聚合、连接、排序等。
- 基于代价的查询优化器决定最佳过滤方案与查询计划，平均性能提升两倍以上。
- 基于蟹行协议实现可供并发访问的 B+ 树索引，能抗住十万并发流量。
- 基于 `redo log` 和 `undo log` 实现了 `STEAL/NO-FORCE` 模式下的回滚与恢复，保证了事务的原子性与持久性。
- 实现了基于 `STEAL/NO-FORCE` 模式下的检查点机制，以加快崩溃恢复例程。
- 实现了行级别的二阶段锁定，基于 `undo log` 引用链实现了 MVCC 功能，保证了事务的隔离性。
- 基于 Netty 实现了客户端与数据库之间高效的通信。



## 工具类

### ByteArray

- ByteArray、ByteList：方便的对字节数组进行写入、读取，以及支持共享数组。

### DbFile

- DbFile：操作磁盘文件的封装，必须是线程安全的。我们假定每一次写入都能完整写入，如果不是，可能必须需要双写技术来保障一致性。

## 堆文件存储

### 表模式 TableDesc

格式以字节数组形式存储在 Catalog 中，它由一个长度接上一个字符串组成：

```yaml
table_name (field_name field_type index_type) (field_name field_type index_type) ...
```

一个字段可能建立多个索引，`index_type` 以 32 位整型表示，他们的低 5 位标识具体的索引类型：`PRIMARY_KEY、BTREE、HASH、BTREE_UNIQUE、HASH_UNIQUE`。

表模式不会存储隐藏字段。

### 目录 Catalog

目录文件是存储 TableDesc 的文件，Catalog 加载 TableDesc 到内存中，并根据 `table_name` 加载表文件，创建 `PageManager`。

Catalog 对外需要暴露根据表名获取 TableDesc 和 PageManager 的接口，以及创建并存储 TableDesc 的接口。

### 行记录 Record

行记录由页 ID 和插槽号唯一标识，行记录在页中的存储是紧凑的，它只需要存储字段值。任何一个字段都是定长的，`String` 字符串也是定长存储的。

任何一个行记录在最后都会有两个隐藏、匿名字段：**一字节的有效位和八字节的回滚指针。**这两个字段不会出现在 TableDesc 中，但他们存储时总会被自动写入。

记录的有效位与插槽不一样，一个记录被删除后，记录有效位清零，但由于 MVCC，很可能其他事务也在引用这个记录，因此他的插槽位不能清零，因此删除只是逻辑删除的，只有当没有事务引用记录时，它才能够被真正删除。

### 堆文件页 HeapPage

任何一个页都是以 表名 + 页号 唯一标识。堆文件页的格式如下：

```yaml
[8b-page_lsn][bit_set] ......
```

它以 8 字节的 LSN 开头，随后跟着一组位图来表示插槽，其余部分存储定长的**行记录的值**。

那么一个堆文件页最多可以存储
$$
floor(\frac{(PageSzie-8) \times 8}{RecordSize \times 8 + 1})
$$

个元组。

任何一个页都必须提供读写锁，注意，**一个事务可能会同时存在多个线程调用，因此，锁是事务级别的，而不是线程级别的。**

### 页面管理 PageManager

每个表文件不同，**PageManager 以表名唯一标识**。PageManager 负责读取和写入页面到磁盘，并且还负责空闲空间管理。

读取和写入只需通过页号定位偏移即可，无需关注具体的页面内容，重点是空间管理，在这里，只有堆文件可能存在并发写入的冲突，索引页面都是锁定页的，因此只需关注堆文件的空闲空间管理。

在 HelloDb 中，页面都是以一些插槽和一些 `entry` 组成的，插槽指示着页面是否存在空闲空间，每一次请求空闲空间页相当于请求一次空闲插槽，PageManager 缓存这些可插入的插槽，一旦事务请求，PageManager 返回一个页面以及对应的插槽，**PageManager 要保证，一旦它最多将用一个插槽分配给一个事务。这样可以保证事务在同一个页面上的并发写入。**

> PLUS：在一切都完成后，尝试修改为 DoubleWrite 读写。

### 页面池 PageBufferPool

页面池是页面的获取页面的唯一入口，能够以指定权限获取页面，并在事务释放时统一释放事务在页面上的锁。

对于堆文件页，由于要实现行级锁定，事务获取堆文件页总是使用 `READ_ONLY` 权限锁定，**缓冲池不允许驱逐正在被事务锁定的页面**，因此缓冲池在驱逐页面时，必须要以超级事务获取页面上的写锁，这样才能保证页面被驱逐时，不会再有事务引用这个页面了。

当缓冲池驱逐时，如果页面为脏页，则它必须要先刷盘再驱逐页面。

我们总是可以基于某些规则保证，**无论是堆文件页还是索引页，事务获取页面时，不会存在死锁，这使得 PageBufferPool 的实现简单的多。**



## B+ 树索引

### BTreeSuperPage

B+ 树文件的第一页作为超级页，最开始一个字节标识索引字段的类型，然后四个字节为 B+ 树根节点的页号，随后页内的每个比特位表示第 i 页是否使用。

需要注意两点：第一个比特位总是表示自身超级页；根节点不存在时，他应该置为 0，因为任何 B+ 树页都是从 1 开始的。

一个 B+ 树索引维护一个不可变的超级页对象，由于根节点是可变的，锁定根节点前，必须要锁定此超级页，而释放根节点锁定时，也必须释放超级页的锁定。超级页不会通过缓冲区获取，超级页的读取、写入、锁定都是通过 BTreePageHolder 直接操作 PageManager 锁定的。

除此之外，超级页还负责空闲页面的分配和回收。

### BTreeInternalPage

B+ 树内部节点存储着一些 key 和指向子节点的指针，具体的，一个 m 阶的 B+ 树，具有 m 个 key 和 m + 1 个指针。

内部节点包含一系列 BTreeEntry，这些 Entry 包含一个 key 和左右指针，第 i 个 Entry 的右指针严格与第 i + 1 个 Entry 的左指针相等，因此，插入、删除、更新必须小心维护 Entry 的左右指针。

由于指针与 key 并不是对应的，例如删除第 i 个 Entry 和它的左指针会导致他前一个 Entry 的右指针也失效，必须要更新前一个 Entry 的右指针为第 i 个 Entry 的右指针。因此，key 与 指针在磁盘上是分开存储的，在内存中也是存储 key 数组和 child 数组，而非存储 Entry，迭代器会将其转为 Entry 遍历。

内部节点第一个字节为页面的类别，然后四个字节是指向父指针的页号，然后是一些插槽，第 i 个插槽表示着第 i 的 key 和指针是否有效，然后是 m 个 key，最后是 m + 1 个指针，key 数组永远从 1 开始计数。

一些操作：

- 删除 Entry：删除一个 Entry 不会改变 Entry 数组的有序性，因此他不会涉及到指针的移动，但这可能会使得中间某些 Entry 是空的。删除一个 Entry 可描述为删除一个 Key 和一个指针，指针可以是左指针或者右指针。当删除右指针时，直接标记插槽 i 为 false 即可，当迭代器工作时，一个 Entry 的左指针会设置为上一个有效的指针（插槽为 true）；当删除左指针时，标记插槽 i 为 false  是错误的，因为插槽 i 表示的是 Entry 的右指针，因此我们需要将上一个有效的 Entry 的右指针设置为 i 的右指针。

- 插入 Entry：插入的 Entry 绝不允许左右指针为空，插入一个 Entry 需要定位到插入点，由于指针会被两个 Entry 共享，因此需要找到一个 child[i]，他等于插入 Entry 的左指针或右指针，我们应当插入到这个位置的后一个。需要注意，如果 child[i] 等于插入 Entry 的右指针，插入后 child[i] 应该更新为 Entry 的左指针。除此之外，我们还需要找到一个空槽位，如果空槽位小于 i，则需要将空槽位到 i 的节点向前移动，然后插入下标 i 处；如果大于 i，则需要将 i + 1 到空槽位的节点向后移动，然后插入下标 i + 1 处。
- 更新 Entry：更新 Entry 需要更新 key、更新左右指针。更新右指针直接更新 child[i] 即可，但是更新左指针需要找到上一个使用的下标更新。
- 迭代器：迭代器用以整和 key 和 child 迭代 Entry，Entry 的 key 和右指针都是 key[i] 和 child[i]，但左指针则是上一个有效的 child[i]。

### BTreeLeafPage

叶子节点存储着实际的 key 和指向行记录的 RecordId，key 和 RecordId 唯一对应，因此存储时它们是一起存储的。

叶子节点第一个字节为页面的类别，然后十二个字节是指向父指针、左兄弟、右兄弟的页号，然后是一些插槽，最后一系列是 BTreeLeafPageEntry，包括 key 和 RecordId。

### BTreePage

叶子节点和内部节点集成的类，封装了一些通用的方法。可通过此类判断具体页面的类型。

### BTreePageManager

负责从页面中读取和写入 BTreePage，读取时会根据页面的类别构造不同的 Page。 

BTreePageManager 同样可以通过 Catalog 获取，PageBufferPool 同样可以利用 BTreePageManager 读取页面，对于 BTreePageManager 而言，他的 `tableName` 被设置成 `tableName-fieldIndex-indexName`，这样可以保证唯一性。

> 规定表名等名称不允许包含特殊字符 “-”

此 tableName 会唯一对应一个 TableDesc、PageManager 和 Index。

### BTreePageHolder

对 B+ 树页面的锁定绝不需要等待事务结束后再释放锁，我们允许一个事务可以看到另一个事务对 B+ 树所做的修改，因为最终会通过 MVCC 保证数据的一致性。

因此，操作结束后，任何页面上的锁都将释放，BTreePageHolder 就是做这件事的，BTreePageHolder 在方法直接传递，他暴露获取页面的接口，并在一次操作结束后统一释放锁。

B+ 树页面没有使用重做日志来维护持久性，或者说除堆页面外的页面都没有，这很棘手，因为堆文件页面会存在 undo 和 redo 来回滚或重做，二这些页面没有。

> 大部分数据库会对索引也记录日志。

但好在，索引页面不需要考虑回滚的操作，这是因为：

- 对于插入操作回滚，我们仅仅只是将记录中的有效位清零，绝不会修改索引页面。
- 对于删除操作回滚，我们仅仅只是将记录中的有效位恢复，绝不会修改索引页面。
- 更新操作绝不允许更新索引上的列，因此也不会对索引页面进行任何的修改。

现在，事情简单多了，由于不需要考虑回滚，页面可以提前写入磁盘。我们只需要实现 FORCE 模式即可，这很简单，一次操作完成后，BTreePageHolder 将任何脏页落盘，并标记干净，然后再释放写锁。在这里，我们总是假定一次操作是原子的，这是个不现实的假定，它可能会导致一些问题，这是此项目的缺陷之一。

这样，由于没有两相锁定，并发性会很高效。但是，IO 写入会成为主要瓶颈。

## 查询运算符

### OpIterator

行记录运算符接口，运算符接受一些子运算符输入，然后可输出为新的迭代器。

**运算符必须要调用 open 后才开始工作，并且要支持 rewind 操作**。这个特性允许我们提前缓存运算符，而不会实际的工作，在执行查询步骤时，这非常重要。

### Filter

Filter 接受一个子运算符，并将子运算符产生的元组中的某个字段与常量过滤，并返回过滤后的迭代流。

### NomalJoin

在大多数连接条件下的 Join 运算符，接受两个子运算符，使用循环嵌套法将它们进行过滤，返回两表外连接的结果。

### HashEqualJoin

在等值连接条件下的 Join 运算符，接受两个子运算符，使用哈希连接法将它们进行过滤，返回两表外连接的结果。

### Aggregate

聚合运算符，接受一个子运算符，对一些列进行聚合分组运算，聚合运算符由 Aggregator 聚合器驱动，最终聚合列的结果，如果存在分组字段，则返回的迭代流元组的第一个字段为分组的列，其余字段为聚合的列（不允许有非分组非聚合的列）。

数字型支持 SUM、MAX、MIN、AVG、COUNT 等聚合运算符，其中 AVG 总是浮点型，COUNT 总是整型，其余运算符结果类型为元组本身类型。

字符串仅支持 COUNT 聚合运算符。

### UnionOnOr

处理 OR 时的运算符，负责将 OR 两边的子运算符结合起来并返回一个新的运算符。

### OrderBy

排序运算符，接受一个子运算符和一个排序字段，以及一个标志位指示是升序还是降序，返回排序后的迭代流。

### BTreeSeqScan

一切运算符的源头，BTreeSeqScan 利用 B+ 树索引，选择是否全表扫描还是基于条件的查询，返回元组迭代流，BTreeSeqScan 必须要**过滤被逻辑删除的元组。**

在后续，BTreeSeqScan 还需要实现**版本控制隔离。**



## 查询优化器

[查询优化实验](./lab3.md)

HappyDb 只允许两表进行一次连接，例如不能 `a.x = b.y AND a.y = b.x` 进行两次连接。

### Histogram

某个字段上的直方图，能够动态添加和删除条目，具有固定的桶数，能够根据谓词估计基数。

仅需实现 int 类型直方图，int 直方图应该要保证桶的宽度至少为 1，否则，对于一些落在小数域的桶，他的高度永远为零。
$$
\frac{(max - min) }{numBuckets} >= 1 \iff numBuckets <= max - min
$$
动态添加删除时，如果超过了 `max、min` 范围时，则截断为 max 或 min。

String 类型和 Double 类型可以直接使用 int 直方图完成，这可能会有一些误差。

Histogram 需要提供了直方对，用以估计连接后的大小。

### TableState

一个表的状态，维护了表中所有字段的直方图，并暴露一些接口：

- 给定字段、谓词，估计选择性。
- 给出扫描表所需要的 IO Cost。
- 给定选择性，估计基数，即乘元组总数量即可。
- 动态添加或删除元组，动态维护各字段的直方图。
- 获取字段的直方对。

### TableStateView

在连接时，我们需要 TableState 来预估表的代价，但是不可能每次都扫描表创建 TableState（可以随机抽样），因此需要一个视图，TableState 只在第一次被创建，而后动态维护。

元组的插入删除需要同时维护视图（更新等效于删除再插入），TableStateView 是操作 TableState 的入口。

一段时间后，误差可能会变大，例如插入了大量不在 `min max` 范围内的元组，因此一段时间后或插入大量元组后，需要重建 TableState。

### CostCard

表示一些表连接预估的代价与基数，CostCard 可以抽象看成一个表，与其他表进行连接。

### JoinOptimizer

查询优化器，需要给出决定最优顺序的接口。

它评估两个表连接的代价，并基于记忆化搜索决出最优顺序，一个集合产生的最优排序是一致的，而一个集合又可以由他的两个子集递归而来。

这里不能分治递归左右子集然后结合（[原因](./lab3.md)），因此我们需要动态规划一步一步构建。

## 查询解析器

### 查询规则

HappyDb 支持的查询类型：

- **最多一个字段上的分组**以及多个字段上的聚合：

```sql
SELECT country, SUM(age) sum_avg, AVG(salary) AS avg_salary FROM people AS p GROUP BY country;
```

最多只允许一个字段上的分组，可以不分组。如果存在聚合字段，那么查询列必须全部是聚合字段或分组字段，不能是其他字段。支持别名输出。

- 无优先级 where 子句，不支持子查询、括号优先级以及表达式，OR 必须放在最后：

```sql
SELECT * FROM t WHERE t.x = 1 AND t.y = 2 OR t.z = 3;
```

请注意，例如 `WHERE t.x = 1 OR t.z = 3 AND t.y = 2` 查询是不允许的，因为语义不清，而且我们暂不支持括号优先级（之后的优化点），以及类似于 `t.x = t.y + 1` 之类的运算表达式，在 Update 中我们会实现这些运算表达式，在这里仅支持单列与常量的对比。

- ORDER BY 对某字段升降序排序，但排序字段最多存在一个。
- 多表内连接，但不支持 ON OR，事实上许多数据库也是如此，这涉及到查询优化，ON OR 可以使用 UNION ALL 替代，但这里不支持：

```sql
SELECT * FROM a, b, c WHERE a.x = b.x AND b.x = c.y;
```

注意，连接仅支持上面语法，不支持 `JOIN ON` 语法，不允许 OR 在连接条件中。另外，每个表最多被连接一次，例如 `a.x = b.x AND a.x >= c.x AND b.x >= c.x` 是不被允许的，前两个条件已经导致了 `abc` 三表连接，因此最后一个条件是错误的。

### 解析步骤

1. 解析 LogicalPlan 查询计划：
   1. 解析 from tables，将 tableId 和 tableAlias 添加到 LogicalPlan 的 tableMap 中。
   2. 解析 where 表达式：
      1. 如果操作符是 AND，则代表是多个表达式，则递归解析每一项子表达式。
      2. 如果操作符是 OR，不支持此操作。
      3. 如果是单个子表达式：
         1. 解析操作符
         2. 判断两边是不是都为表的字段，如果是的话则为 JOIN 操作：
            1. 解析左右字段名，调用 LogicalPlan addJoin 操作。
            2. 如果没有给出别名，则遍历表，判断字段属于哪个表，将字段名更新为 table.fieldName 的形式。
            3. 将表别名与字段名作为构造函数，构造 LogicalJoinNode，添加到 joins 中。
         3. 否则，为一个 Filter 操作：
            1. 解析字段和常量，调用 LogicalPlan addFilter 操作。
            2. 如果没有给出别名，则遍历表，判断字段属于哪个表，将字段名更新为 table.fieldName 的形式。
            3. 将表别名与字段名作为构造函数，构造 LogicalFilterNode，添加到 filters 中。
   3. 解析 group by 表达式：
      1. 如果存在分组，那么 select 要么是分组字段，要么是聚合表达式。
      2. 将聚合操作符、聚合字段、分组字段添加到 LogicalPlan 中，字段名更新为 table.fieldName 的形式。
      3. 仅支持一个分组和一个聚合字段。
   4. 解析 select 字段：
      1. 将字段和聚合函数添加到 selectList 中，聚合函数允许为空。
      2. 如果是 select * xxx，则字段名为 `null.*`。
   5. 解析 order by 字段：
      1. 只允许一个 order by 字段，将字段名更新为 table.fieldName 的形式。
      2. 解析升序和降序字段。
2. 将 LogicalPlan 逻辑计划转换为 OpIterator 物理计划：
   1. 对每个表进行全表扫描，将子运算符放入 subplanMap 中，key 是表的别名，val 是 SeqScan 运算符。
   2. filterSelectivities 选择性因子每个表初始化为 1.0，key 是表，val 是选择性因子。
   3. 遍历 filters，构建 Filter 运算符，用 Filter 运算符覆盖 subplanMap。
   4. 选择性因子与 filterSelectivities 中的原先的因子相乘（仅支持 AND）。
   5. orderJoins 决定出最佳的连接顺序。
   6. 遍历最佳连接顺序，每一个连接都是 `a:b` 的形式：
      1. 首先获取两个表的名称，如果 equivMap 包含表名，则以 equivMap 中的为准。
      2. 根据表名，从 subplanMap 中获取子运算符。
      3. 构建连接 Join 运算符，覆盖表 a 的 subplanMap。
      4. 修改 equivMap，将表 b 以及所有 val 为表 b 的表修改为表 a。
   7. 如果存在聚合，则将子运算符传递构造分组运算符。
   8. 如果存在排序，则将子运算符传递构造排序运算符。
   9. 遍历 select 列表，构造 ouyTypeList 和 outFieldList。
      1. 这两个列表是针对最终子运算符的而言的，表示要投影子运算符 td 中的第几个字段。
      2. 将子运算符和列表传递给 project，project 最终构造投影。





## 回滚与恢复

### UndoLog

一个 UndoLog 包括对应行记录的前镜像值，以及行记录的 ID、以及事务的 ID。

在这里，规定每个表中的 UndoLog **存储在不同的文件中**，UndoLogPageId 的表名由真实表名 + `-undo` 后缀组成。

一个 UndoLog 的大小是固定的，它由 `UndoLogNo + Record + Record PageNo + RecordNo + XID` 组成。其中 UndoLogNo 是指他是事务 XID 按顺序产生的第几个日志，一共 4 + 4 + 4 + 8 + record.size 字节。

在 Mysql 中，不同的 UndoLog 由不同的 UndoLogPage 存储，为了提供并发效率，一个事务所有的 UndoLogPage  会指定段分配，这些 UndoLogPage 以链表形式被组织。

但这里，为了简便，我们统一了 UndoLog 的格式。

理论上，我们应该区分 Insert undo 和 Undate undo，因为 Insert undo 没有 MVCC，可以直接删除（它的上一个版本是空，删除它还是空），但这里为了简便，没有做区分。

除此之外，UndoLog 还具备一个引用计数，标识某些事务正在引用这个版本，这个字段不会被序列化存储。

事务提交后，事务对应的 UndoLog 删除不能被立即删除，它会被加入到 HistoryList 中等待删除：

- 遍历所有活跃事务最早 ReadView，判断是否可见此事务，如果可见则：
  - 遍历 UndoLog，引用计数为 0，则可以删除 UndoLog。

一个数据记录的删除也与此类似，之前都是逻辑删除，现在可以完善这个概念：

- 判断事务产生的 UndoLog 是否可被删除，如果可以，则此记录也可以被删除。



### UndoLogPage

继承 Page 接口，实现与 HeapPage 几乎一致。



### UndoLogSuperPage

UndoLogPage 的第一（零）页被设为超级页。第 i 个比特位表示事务 ID 为 i 的事务是否在此 UndoLogPage 上存在未提交日志。

维护 UndoLogSuperPage 是 RedoLogBuffer 的责任。



### UndoLogPagePanager

类似于 HeapPageManager，具备 malloc 和 free 功能，可由 Catalog 获取，tableName 是真实的表 + “-undo” 后缀组成的。可由此表名获取真实表的 TableDesc。



### InsertRedoLog

插入重做日志。格式为：

```yanl
[totalSize-int][logType-1byte][lsn-long][xid-long][tableNameLen-int][tableName][pageNo-int][recordNo-int][data]
```

注意，插入不一定是针对行记录，undolog 同样需要 redo log。insert 可以包括 undo log 的重做，由 logType 指定。

**totalSize 指除 totalSize 4 字节外的其他数据长度。** redo log 的总长度为 totalSize+ 4。



### UpdateRedoLog

格式与 InsertRedoLog 一致。



### DeleteRedoLog

```yaml
[totalSize-int][logType-1byte][lsn-long][xid-long][tableNameLen-int][tableName][pageNo-int][recordNo-int]
```

删除日志不需要记录具体数据。



### AbortRedoLog

记录事务回滚操作，恢复时需要遍历 UndoLogPage 然后获取有关事务的所有 UndoLog，逆序回滚。

```yaml
[totalSize-int][logType-1byte][lsn-long][xid-long] 
```



### LogBuffer

RedoLog 由 LSN 唯一标识，LSN 每次自增一个 RedoLog 的大小，因此写入可以将 LSN 作为写入偏移，但重做日志大小不能无限增长，当检查点发生时，我们需要进行截断，对于写入，需要计算写入偏移，假定文件头大小为 `HeadSzie`，目前已经截断了 `T` 字节：
$$
offset = LSN - T + HeadSize
$$
RedoLogBuffer 的头部由 24 字节组成：`MAX_LSN、CHECK_POINT、T`。

RedoLogBuffer 要做的事情比较多：

- 负责创建对应的日志，包括 redo 和 undo，创建 undo 时还需要创建 redo 以及修改  UndoSuperPage。
  - 当创建一个 redo log 时，需要向磁盘中写入占位日志来保证日志连续，占位日志只包括 `totalSize + type` 5 字节，不会特别影响性能。
  - 关于 Record 必须要使用他的 clone()，RedoLog 中的 data 不允许被修改。UndoLog 本身就是不可变的。
  - 更新页内的 LSN，并放入 flushList 供检查点刷回，原理请参考检查点机制。
- 提供事务提交的接口，将事务产生的所有 redo 刷盘，将所有 undo log 加入历史链表，并修改 UndoSuperPage。
- 提供事务回滚的接口，逆序重做所有的 undo log，生成 AbortRedoLog 并落盘，丢弃事务所有 redo log。
- 提供设置检查点的接口，并将检查点之前的日志全部丢弃（截断）。
- 提供迭代器，遍历从检查点后的所有日志并忽略占位日志。



### CheckPoint 检查点

检查点在某些时候将脏页刷新回磁盘，在恢复时，需要从检查点扫描到写入点。

检查点并非是按 LSN 顺序将脏页刷盘后最新的 LSN，例如考虑一个复杂的情景：

> 事务 A 修改页面 X LSN 为 100，对应日志 x1；然后修改页面 Y LSN 为 200，对应日志 y1；然后修改页面 X LSN 为 300，对应日志 x2。
>
> 此时模糊检查点发生，此时页面 Y -> 200，X -> 300，因此先将页 Y 刷回，并丢弃 200 及以前的日志。
>
> 这是错误的，这样的话 x1 就被丢弃了！

**因此，flushList 必须要按照脏页最小的 LSN 计算。**

这要怎么实现呢？一个页应该要有两个 LSN，一个记录第一次 LSN 修改，另一个记录最新的 LSN。

检查点刷盘时，按照脏页第一次 LSN 从小到大排序刷回，但是恢复时，应该比对页最新的 LSN。即将页面由干净弄脏时记录的第一次 LSN 仅在内存中供检查点机制使用。

为此，我们提供一个接口 `DataPage`，它具有：

1. setLsn
2. getLsn
3. getFristDirtyLsn
4. markDirty
5. isDirty

几个接口，其中 markDirty 的实现必须要记录下页面由干净变脏时的 LSN，供 getFristDirtyLsn 返回。

markDirty 由 LogBuffer 调用，每一个日志的产生都会假定页面变脏，在 markDirty 前，必须要先设置页面 LSN，其余地方不允许调用 **markDirty(true)**。为了保证一致性，必须先修改页面再产生日志。

> 如果先产生日志，这会使页面变脏，但实际上页面还未被修改。
>
> 如果先修改页面，但并未标记脏页，产生日志之后才会标记脏页，放入 flushList。
>
> 因此修改页面的正确顺序是：产生 undo log，设置隐藏字段，修改页面，产生 redo log。



### Recovery

恢复例程在启动数据库时运行：

- 从检查点开始遍历重做日志：
  - 如果重做的页面 LSN 小于 AbortRedoLog 的 LSN，则进行重做。
  
  - 最后对所有未完成的事务执行回滚操作，由于这些事务未提交，他们的修改的行不会被其他事务修改（行锁），因此可以安全的回滚，并标记事务已取消。
  
    由于这些事务未提交，因此 redo 可能丢失，因此 undo 也可能丢失，这有个简单的解决方案：
  
    > 那就是在修改数据页的时候先产生 undo log，这样 undo log page 的 first dirty lsn永远比对应数据页的 first dirty lsn 小，因此在刷盘时，总是会 undo log page 先于数据页刷盘，这样就能保证：
    >
    > 如果某个行记录刷盘，那么它对应的 undo log page 一定刷盘，不需要 redo 来保证。
    >
    > 如果 undo log 丢失，那么数据页必然未刷盘，自然就达到了回滚的目的。
  
    但是由于索引已经建立，因此在查询时，如果元组不存在，需要插入一个永久删除的元组，此元组对所有事务可见。因此在恢复时，需要对未完成事务扫描一次表。
  
    这是个非常大的开销，但由于本项目对索引的简化，这种开销是必然的，好在 UndoLogSuperPage 可以快速告诉我们事务是否在对应表上产生了 undo。
  
- 释放所有的锁，执行激烈检查点将所有页面刷盘（可选的操作）。



### TransactionManager

事务管理器，分配全局递增事务 ID，并统筹事务回滚和提交时需要干的事情。

事务文件格式为：16 字节的头部，头存储着系统最后的事务 ID xid + 1，然后 8 字节表示系统活跃事务的数量。

随后跟着 xid + 1 字节，每个字节 0 、1、2，指示他们取消、活跃或提交。

- 事务提交
  - 日志刷盘（这一步骤完成后，后面应该不会出现问题）
  - 释放所有行锁
  - 释放缓冲池的所有锁
  - 标识事务提交，写入文件
  - 释放 ReadView
- 事务回滚
  - 基于日志回滚
  - 写入 AbortRedoLog
  - 修改事务页面 LSN，标记脏页
  - 释放所有行锁
  - 释放缓冲池的所有锁
  - 标识事务取消，写入文件
  - 释放 ReadView



## 行记录锁与版本控制

### LockTable

针对行记录，实现事务独占锁，并实现死锁检测。

### ReadView

- trxList：活跃的事务列表
- upLimitId：活跃的事务列表中最小的事务 ID，若无则为 -1
- lowLimitId：尚未分配的下一个事务 ID
- currentId：申请 ReadView 的事务 ID

判断对事务 x 可见性时：

- 如果 x == currentId，可见
- 否则，如果 x >= lowLimitId，不可见
- 否则，如果 x < upLimitId，可见
- 否则，如果 trxList.contains(x)，不可见
- 否则，可见



## 更新操作

### CreateTableParser

HappyDb 只存储基本的表模式，因此约束、默认值、自增主键都是不支持的，数据结构仅支持 `int、double、char`，其中 char 固定为 256 字节长度。

```sql
CREATE TABLE `tb` (
    x int,
    y double,
    z char,
    PRIMARY KEY(x) USING BTREE,
    KEY `index_y_hash` (y) USING HASH,
    UNIQUE KEY `index_y_unique_btree` (y) USING BTREE
)
```

支持创建索引，一个表必须要有一个主键，主键必须要使用 B+ 树。一个字段可以同时建立哈希和 B+ 树两种索引，但一个字段**不能**具备唯一和非唯一两种特性。

语法上需要注意，主键 PRIMARY KEY 不能具备名称，而其他索引必须要带有名称，名称可以是任意的，这仅仅只是为了通过 `jsqlparser` 的解析，HappyDB 不会用到这个属性。

> 2022-12-5：字符串字段上建立 B+ 树在多线程条件下可能会导致 BUG，之前测试时没测试

### UpdateOperateScan

接受 LogicalFilterNode 列表作为参数

UpdateOperateScan 返回的 Record 必须与底层 Record 一致，并具备 TableDesc。

> 此类针对更新操作（Update 和 Delete）进行记录扫描
> 此类接受更新语句的 WHERE 过滤器，以最佳方式获取行记录，此类将对这些行记录加锁(意味着可能会堵塞)，然后作为运算符输出这些行记录
> 此类与 BTreeSeqScan 和 LogicalPlan.decideBestSubPlan(Map, Map, String, List, TransactionId) 具备类似的逻辑，可以复用大多数逻辑，但是有一些细微的差别
> 此类绝不会使用 MVCC，当一个事务正在对一个行记录进行更新操作时，此类不应该对其上一个版本操作，而是应该堵塞在行锁上，等待其他事务操作完毕
> 由于堵塞，当其他事务提交后，本事务等待的行记录可能不再符合过滤条件，因此获取锁后，此类将过滤那些不再符合条件的记录，并释放锁
> 当调用 open() 时，此类会开始获取锁的逻辑，一旦检测到死锁，则会抛出 happydb.exception.DeadLockException
> 此类会过滤掉任何被逻辑删除的记录，一旦过滤操作发生，此类将立即释放记录上的锁
> 过此类产生的记录流指向页面中的记录，对迭代产生的记录修改将会反应到对于页面中



### Update

UPDATE 支持简单运算表达式，增删改都必须只能是单表上的过滤：

```sql
UPDATE t SET x = x * 2 + y + 1 WHERE x = 1 OR x = 2;
```

`x = x * 2 + y + 1 ` 是简单表达式，除此之外，还支持括号运算符。

UPDATE 接受 UpdateOperateScan 作为输入运算符，以及 `x = x * 2 + y + 1` 表达式，UPDATE 负责对输入元组进行更新。

在更新时，UPDATE 运算符应该要产生 undo 和 redo 日志。

任何更新操作运算符只返回一行记录，记录只有一个字段，那就是 `rowsAffected` ，即操作影响的行数。



### Delete

Delete 相对较为简单：

```sql
DELETE FROM t WHERE ....
```

DELETE 同样接受 UpdateOperateScan 作为输入运算符，并对这些元组进行逻辑删除，

在删除时，DELETE 运算符应该要产生 undo 和 redo 日志。

删除仅仅只是逻辑删除，因为可能有 MVCC 正在引用行记录，应该由 LogBuffer 在日志提交或回滚时将这些待删除的记录交由 PURGE  线程处理。



### Insert

插入初步支持 VALUES 的写法：

```sql
INSERT INTO t(x, y) VALUES(1, 2);
INSERT INTO t VALUES(1, 2);
```

Insert 不支持 NULL 值，如果未指定某个字段，字段将被设置为自身的默认值，int、double 为 0，string 为空值。

插入运算符一个麻烦点在于唯一索引的处理，多线程并发插入可能会存在问题，因此这里需要锁，判断重复与插入是一个原子的操作，但是问题在于，可能有多个唯一主键，因此需要一个表锁。

由于记录可能存在逻辑记录，可能有些棘手，例如其他事务逻辑删除了行，此时绝不能认为没有行记录，因为其他事务可能会回滚，因此需要遍历可能的行记录，如果记录上的事务 ID 不可见，则认为存在记录，抛出异常。

<ul>
              <li>使用索引搜索等值条件的记录 ID 列表，如果存在任意一个行记录被其他事务锁定，则认为已有记录，抛出异常</li>
              <li>否则，如果存在记录，其事务 ID 对本事务不可见(使用当前读)，则认为已有记录，抛出异常</li>
              <li>否则，如果存在记录有效位为真，则认为已有记录，抛出异常</li>
              <li>否则，可以安全的插入</li>
          </ul>



## CS 通信架构

参考：[https://github.com/happysnaker/HRpc](hrpc)



## Raft 主从复制与故障恢复

关系型数据库不像 kv 数据库一般，关系型数据库数据同步是基于事务的，复制前的主要逻辑如下：

1. 接受客户端请求时，如果自身不是主库，则将消息重定向给主库执行。
2. 对于主库而言，如果不是 begin 语句并且客户端通信 serverID 不是自己，直接报错，并回滚事务。如果是 begin 则设置 serverID 为自己。
3. 所有 SQL 语句连带事务 ID 都必须缓存起来。
4. 事务提交时，将事务所有执行的语句整和成 RaftLogEntry，调用 Leader 模块进行日志同步。
5. 若日志同步成功，则提交事务，然后发送提交信息给从库；否则，则回滚事务。

> 与 Mysql 主从复制的区别：Mysql 主从复制是在事务提交后，写入 binlog 文件，随后由 dump 线程拉取 binlog，异步或半同步复制给从库，从库使用 IO 线程接受并写入 delay log，由 SQL 线程从 delay log 中读取并应用。
>
> 这里的区别在于 Mysql 是已经做完了再去同步，这就会导致其他从库可能没有应用成功，从而导致某一时刻状态的不一致，Mysql 主从复制不保证一致性，或者说，一致性很差，从库可能落后主库很多，不同从库之间的状态也可能是不一致的，但是可用性、性能都非常好，几乎不会因为复制产生额外的开销。
>
> Raft 算法核心在于，在做之前，判断能不能做，如果能做再去做，一致性非常强。但如果没有半数以上节点统一的话，可能会导致 Mysql 不可用。同时由于同步，并且事务可能包含多条语句，执行的性能是非常非常低的。Raft 的一致性并不是线性一致性，而是主库与从库之间的数据一致，对用户而言，其看到的数据很可能是不一致的。
>
> 可以看成 AP 和 CP 的差别。数据库向来要保证高性能、高可用，绝大多数情况，系统的性能瓶颈大多是出现在数据库上，因此大多数数据库同步复制都没有保障强一致性。此处使用 Raft 仅仅只是为了学习。

为了简便，此处不会实现日志压缩与成员变更模块，事实上关系型数据库无法像 kv 数据库那样进行日志压缩，而成员变更模块则是暂时不考虑实现。



### LogEntry

Raft 日志，包含一整个事务的执行语句、以及日志产生时 Leader 任期。



### RaftLogManager

任何节点都要有 RaftLogManager，它保存 LogEntry 数组，在 Raft 模块初始化时，RaftLogManager 从磁盘中加载日志到内存中，这可能会导致内存膨胀，因此内存中最多装载一定数量的日志，如果内存中查不到才会从磁盘中加载。

RaftLogManager 需要维护三个变量：

- maxIndex：当前 LogEntry 最大的索引编号，编号可能减小，日志可能会被主节点日志覆盖。
- commitIndex：已经连续提交的 LogEntry 最大的索引编号，已被提交的日志不可能再被覆盖。
- lastApplied：上一次应用到状态机的 LogEntry 的索引编号，lastApplied <= CommitIndex。

RaftLog 文件以 12 字节打头，12 字节为上面三个变量，然后跟着 MaxIndex 个 BinLog，BinLog 格式为多个字符串，格式如下：

```yaml
[xid-long][size-int][sql1][size-int][sql2]...[totalSize-int]
```

totalSize 存在末尾，因为对 BinLog 的读取总是从末尾向前读取的，totalSize 不包含自身 4 个字节。xid 可能为 -1 代表未持有事务。

应用到状态机的规则是：

应用的规则是：

- 如果 Log 未存在 xid，则开启事务，将事务 ID 写入 Log 文件中，然后重放事务执行。事务执行完毕，lastApplied 自增。
- 如果 Log 存在 xid，如果事务处于回滚状态，这种情况重新开启事务，重新写入 Log 文件，重放事务执行即可。
- 如果 Log 存在 xid，如果事务处于活跃状态，则提交事务，lastApplied 自增。
- 如果 Log 存在 xid，如果事务处于提交状态，这是由于事务执行完毕，lastApplied 忘记自增而突然宕机，这种情况由于事务已经执行完毕，lastApplied 自增即可。



### RaftConfig

RaftConfig保存着所有节点的状态以及当前主节点，同时也是获取 RaftNode 的唯一入口，因为 RaftNode 可能随时会改变。

RaftConfig 暴露刷新节点的接口，为保障一致性，如果调用者不是当前节点，将静默。



### RaftNode

RaftNode 是 Raft 算法中的抽象节点，它具备如下几个重要接口，所有子节点的实现一致：

- appendLogEntries：来自主节点的追加日志 RPC 调用，此 RPC 调用同时作为心跳包使用。

  - 如果对方任期小于接收者当前任期，返回假，并告知领导人当前的任期，让领导人转为 Follower。
  - 如果对方任期大于自身，如果自身不是 Follower，则转为 Follower 追随对方，重试方法。
  - 如果自身 prevLogIndex 处的 Log 的任期与对方发送过来的不符合，返回假。
  - 向 RaftLogManager 中顺序追加日志，如果 RaftLogManager 中某条日志与 Leader 发送的日志的任期不符合，则丢弃此条日志及之后的日志，以 Leader 发送过来的日志覆盖。返回真。
  - 如果对方的 commitIndex 大于自身的 commitIndex，则更新自身 commitIndex 为 `Math.min(leaderCommitIndex, maxIndex)`，同时递增 lastApplied 并将日志应用到状态机中。
  - 重置计时器。重置计时器可以使用一个简单的方法，即刷新状态为 Follower。

- requestCommit：来自领导人的提交请求，集群已经就这些待提交的日志达成了共识。

  - 如果对方任期小于接收者当前任期，返回假，并告知领导人当前的任期，让对方转为 Follower。
  - 如果对方任期大于自身，则转为 Follower 追随对方，重试方法。
  - 如果待提交的日志任期与对方的不一致，静默返回。
  - 否则，如果对方 commitIndex 比自身大，则更新自身 commitIndex0，并推进 lastApplied。

- requestVote：来自候选者的投票请求。

  - 如果对方 term 小于自身，返回假。
  - 如果对方 term 等于自身，但自身选票给了别人，返回假。
  - 否则，比较对方最新日志的任期与自身最新日志的任期，如果小于自身，返回假。
  - 否则，比较对方最新日志的下标与自身最新日志的下标，如果小于自身，返回假。
  - 否则，更新自身任期，并将状态改为 Follower，返回真。
  
  

### Leader

Leader 继承 RaftNode，作为 Raft 集群中的主节点，Leader 的实现要求如下：

- 在初始化过程中，启动心跳例程，定期向其他节点发送心跳包请求。
  - 节点会等待一段时间接受其他节点的响应（超时则不管），如果对方任期比自身大，尝试切换成 Follower，切换的过程要获取 RaftConfig 的锁并进行双重验证。
  - 如果 RaftConfig 中的  raftNode 不等于自身，则销毁自身（可能已被销毁），取消定时任务。

- 事务提交时同步日志，首先写入 RaftLogManager 中，然后开始同步日志。
  - 异步向每个节点发送从 nextIndex（初始为 maxIndex + 1） 到 currentIndex 的日志。
  - 同步等待节点答复：
    - 如果节点任期大于自身，则刷新为 Follower，并返回 false 告知事务回滚。
    - 如果节点返回 false，则节点的 nextIndex 递减，继续同步日志。
    - 如果节点返回 true，则记录下来，并更新节点的 matchIndex 和 nextIndex，若半数以上节点同意，则视为同步成功。
    - 若最终失败，则应该重试，Raft 为了一致性会放弃可用性。
  - 枚举 N，假设存在 N > commitIndex，使得大多数的 `matchIndex[i] ≥ N` 以及 `log[N].term == currentTerm`  成立，则令 `commitIndex = N`。
  - 应用到状态机中，推进 lastApplied。
  - 异步发起 requestCommit 请求告知其他从节点提交，参数为当前 commitIndex 以及对于 commitIndex 的日志任期。

这里存在一个 Raft 算法的缺陷问题，考虑如下场景：

> 有五个节点 ABCDE，初始 A 为主节点，A 收到客户消息 X，写入日志并开始同步日志，假设 B 收到的日志而 CDE 均未收到日志，此时节点 C 超时，开始选举，它赢得了 CDE 的选票成为了任期更大的主节点。
>
> 于是节点 A 同步消息失败，告知客户端写入失败。但是一段时间后 A 又成为了主节点，在同步日志时他会顺带提交其他任期的日志，于是消息 x 又被提交。
>
> 对于客户端而言，这是收到欺骗的表现，A 明明告诉他消息执行失败了！

上面这种情况是可能发生的，无法避免，但能进行优化，即节点成为主节点的第一件事就是同步一个空日志，同步这个操作会覆盖其他节点上不同与主节点的日志。但是问题仍然是存在的，因此 Raft 大多用于对于日志同步这种要求不是那么严格的场景，关系型数据库几乎不会使用 Raft。



### Follower

Follower 继承 RaftNode，作为 Raft 集群中的主节点，Follower 的实现要求如下：

- 初始化时设置超时计时器。
  - 如果不存在说明主节点超时，销毁自身，自增任期，刷新为 Candidate，进行候选。
  - 如果收到心跳，自身会被销毁，刷新 Follower 状态。



### Candidate

Candidate 继承 RaftNode，作为 Raft 集群中的主节点，Candidate 的实现要求如下：

- 初始化时设置超时计时器，调用竞选例程。
  - 如果超时则继续自增任期，刷新为 Cabdidate，重复进行候选。
  - 如果收到心跳或竞选成功，自身会被销毁，刷新为其他状态。
- 发起 RPC 投票请求，附带自身最新日志的编号和任期。
  - 异步发起投票。如果有半数以上节点响应，刷新为 Leader。
  - 否则静默等待超时。
