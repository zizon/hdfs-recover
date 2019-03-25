# hdfs-recover
something to deal with hdfs trash

# EditLogTailer
to tail edit log of certain nameservice,invoke like
```java
// name service to tail.
// test-cluster is the logical nameservice name,
// host parts are the rpc ip and ports of each namenode
URI nameservice = URI.create("test-cluster://10.202.77.200:8020,10.202.77.201:8020");

// storage dir to persist tailed editlog
File storage = new File("__storage__");
new EditLogTailer(
   // the editlog archive manager
  new EditLogArchive(storage),
  
  // the editlog aggregator, fetch from each journale.
  // assume namenode use 50070 as their http listening port
  new LogAggregator(new NamenodeRPC(nameservice, "hdfs"), true),
  
  // editlog op filter
  (op) -> {
      // reject not rename op
      if (op.opCode.compareTo(FSEditLogOpCodes.OP_RENAME_OLD) != 0) {
          return false;
      }

      // skip if not in trash
      if (!RenameOldOpSerializer.target(op).contains(".Trash")) {
          return false;
      }
      return true;
  },
  
  // serialize method for your interested op.
  // will use it to persist editlog to `__storage__`
  RenameOldOpSerializer::lineSerialize,
  (stat) -> {
      // stat info of each editlog ops in on tailing/polling period
      LOGGER.info("stat:" + stat);
  }
).start(
  // journal poing interval
  TimeUnit.MINUTES.toMillis(1),
  
  // keep editlog not older than this
  TimeUnit.DAYS.toMillis(365)
).logException().join();
```

# TailingService
a deamon like EditLogTailer, to use it, one need
1. prepare a config.property like: 
```
nameservice=sfbdp1://10.116.100.3:8020,10.116.100.4:8020
runas=hdfs
poll_period:60000
expiration_for_log:86400000
http_port=10088
```
place it in the top classpath,so daemon can find and read/using it  


2. (Optional) a log4j2.properties to serve log4j2 logging system this daemon use.
```
#example 
#OFF/FATAL/ERROR/WARN/INFO/DEBUG/TRACE/ALL
status=warn
verbose=true
dest=err

# auto reconfigurate
monitorInterval=60

#property.${name}=${value}
#property.logfile=log/runtime.log
property.patten=%date %highlight{[%level]} %logger@Process-%processId(Thread-%threadName/%threadId): %message %n
property.logpath=logs
property.logfile=runtime.log
property.logarchive=logs/archive
# for java logging level mapping
#customLevel.info=400

#filters=${filter_1},${filter_2}

#filter_1 example
#filter.filter_1.type=
#filter.filter_1.onMatch=
#filter.filter_1.onMisMatch=

#appenders={appender_1},{appender_2}

#appender_1 example
#appender.appender_1.name=appender_name
#appender.appender_1.type=
#appender.appender_1.filters=${like top level filters}
#appender.appender_1.layout=
appender.default.name=default-appender
appender.default.type=Console
appender.default.follow=true
appender.default.layout.type=PatternLayout
appender.default.layout.pattern=${patten}

appender.rolling.name=rolling-appender
appender.rolling.type=RollingFile
appender.rolling.fileName=${logpath}/${logfile}
appender.rolling.filePattern=${logarchive}/${logfile}.%d{yyyy-MM-dd}.%i
appender.rolling.layout.type=PatternLayout
appender.rolling.layout.pattern=${patten}
appender.rolling.strategy.type=DefaultRolloverStrategy
appender.rolling.strategy.max = 10
appender.rolling.policies.type = Policies
appender.rolling.policies.starup.type=OnStartupTriggeringPolicy
appender.rolling.policies.starup.minSize=0
appender.rolling.policies.time.type=TimeBasedTriggeringPolicy
appender.rolling.policies.time.interval=1
appender.rolling.policies.time.modulate=true
appender.rolling.policies.size.type=SizeBasedTriggeringPolicy
appender.rolling.policies.size.size=100MB

#loggers=${logger_1}${logger_2}
#logger_1 example
#logger.logger_1.name=
#logger.logger_1.level
#logger.logger_1.type=asyncLogger
#logger.logger_1.appenderRefs=${ref_one}
#logger.logger_1.filters=${like top level filters}

rootLogger.level=info
#rootLogger.type=asyncRoot
rootLogger.appenderRef.console.ref=rolling-appender
#rootLogger.appenderRef.file.ref=rolling-appender

#rootLogger.appenderRef.ref_one=
#rootLogger.filters=${like top level filters}
logger.testserver.name=com.sf.misc
logger.testserver.level=info

#logger.protocol.name=com.sf.misc.antman.ProtocolHandler
#logger.protocol.level=info

#logger.tailer.name=com.sf.misc.hadoop.recover.EditLogTailer
#logger.tailer.level=info
```

3. start it some way like:
```bash
#!/bin/bash
save_dir=`pwd`
current=`dirname "${BASH_SOURCE[0]}"`

jps  |grep TailingService | awk '{print $1}' | xargs kill -9

cd $current
JVM_OPTIONS="-server -Xmx1G -XX:+UseParallelGC -XX:+UseParallelOldGC"
nohup java $JVM_OPTIONS -cp .:hdfs-recover/* com.sf.misc.hadoop.recover.TailingService > run.log 2>&1 &

cd $save_dir
```

4. recover
in case one need to replay editlog between ,said [from,to] ,launch *com.sf.misc.hadoop.recover.ReverseReplay*.
```bash
# with test-cluster as what it said in EditlogTailer.
# false indicate not actually running reverse replay(means move .Trash files&directory back to where they belongs, before moveing to trash), but print the move that will apply.
# 20190318000000 and 20190318201532 are the time range of eidtlogs that should consider to be replay,in yyyyMMddHHmmss format.
java -cp $classpath com.sf.misc.hadoop.recover.ReverseReplay test-cluster://10.202.77.200:8020,10.202.77.201:8020 false 20190318000000 20190318201532
```
