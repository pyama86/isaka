package isaka

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

type Zookeeper struct {
	conn        *zk.Conn
	clusterName string
}

func NewZookeeper(host, clusterName string, timeout time.Duration) (*Zookeeper, error) {
	c, _, err := zk.Connect([]string{host}, timeout)
	if err != nil {
		return nil, err
	}
	return &Zookeeper{
		conn:        c,
		clusterName: clusterName,
	}, nil

}

func (z *Zookeeper) TopicList() ([]string, error) {
	children, _, err := z.conn.Children(fmt.Sprintf("/%s/brokers/topics", z.clusterName))
	if err != nil {
		return nil, err
	}
	return children, nil
}

func (z *Zookeeper) BrokerList() (KafkaBrokersInfo, error) {
	children, _, err := z.conn.Children(fmt.Sprintf("/%s/brokers/ids", z.clusterName))
	if err != nil {
		return nil, err
	}

	var brokerConfigs KafkaBrokersInfo
	for _, child := range children {
		var brokerConfig KafkaBrokerInfo
		b, _, err := z.conn.Get(fmt.Sprintf("/%s/brokers/ids/%s", z.clusterName, child))
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(b, &brokerConfig); err != nil {
			return nil, err
		}
		brokerConfig.ParseEndpoint()
		brokerConfigs = append(brokerConfigs, brokerConfig)
	}

	return brokerConfigs, nil
}
