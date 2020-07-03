# isaka

Isaka is log reader for Apache kafka.

## insatall
```
brew install pyama86/homebrew-isaka/isaka
```

## usage

```
$ isaka --topic <topic name> --cluster <cluster name> --zookeeper <zookeepr host>
```
```
Usage:
  isaka [command]

Available Commands:
  help        Help about any command
  tail        tail log from choose topic
  topic-list  show topic list

Flags:
  -c, --cluster string          cluster name(Env:ISAKA_CLUSTERNAME) (default "cluster")
  -h, --help                    help for isaka
      --kafka-timeout int       kafka timeout (default 10)
  -z, --zookeeper string        zookeeper host(Env:ISAKA_ZOOKEEPERHOST)
      --zookeeper-timeout int   zookeeper timeout (default 10)

```

## Author
- @pyama86
