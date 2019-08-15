# Mapreduce
Mapreduce经典案例  

## User Location
### 现有如下数据文件需要处理
格式：input.csv  
字段：用户ID，位置ID，开始时间，停留时长(分钟）  
4行样例：
```
UserA,LocationA,2018-01-01 08:00:00,60
UserA,LocationA,2018-01-01 09:00:00,60
UserA,LocationB,2018-01-01 10:00:00,60
UserA,LocationA,2018-01-01 11:00:00,60
```
### 解读：
样例数据中的数据含义是：  
用户UserA，在LocationA位置，从8点开始，停留了60分钟  
用户UserA，在LocationA位置，从9点开始，停留了60分钟  
用户UserA，在LocationB位置，从10点开始，停留了60分钟  
用户UserA，在LocationA位置，从11点开始，停留了60分钟  

### 该样例期待输出：
```
UserA,LocationA,2018-01-01 08:00:00,120
UserA,LocationB,2018-01-01 10:00:00,60
UserA,LocationA,2018-01-01 11:00:00,60
```
### 处理逻辑：
1 对同一个用户，在同一个位置，连续的多条记录进行合并  
2 合并原则：开始时间取最早时间，停留时长加和
