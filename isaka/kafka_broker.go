package isaka

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/thoas/go-funk"
	"go.uber.org/multierr"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
)

type KafkaBrokers struct {
	brokersInfo KafkaBrokersInfo
	listener    string
}

func NewKafkaBrokers(brokersInfo KafkaBrokersInfo, listener string) *KafkaBrokers {
	return &KafkaBrokers{
		brokersInfo: brokersInfo,
		listener:    listener,
	}
}

func (k *KafkaBrokers) Reader(topic, groupID string, tail int64) (*kafka.Reader, error) {
	o, err := k.lastOffset(topic)
	if err != nil {
		return nil, err
	}

	offset := o - tail

	group, err := kafka.NewConsumerGroup(kafka.ConsumerGroupConfig{
		ID:      fmt.Sprintf("isaka-%s-%s", topic, groupID),
		Brokers: k.brokersInfo.BrokerEndpoints(k.listener),
		Topics:  []string{topic},
	})

	if err != nil {
		return nil, err
	}

	defer group.Close()

	gen, err := group.Next(context.TODO())
	if err != nil {
		return nil, err
	}

	topicAssingment := gen.Assignments[topic]

	overrideCommit := map[string]map[int]int64{}

	for _, v := range topicAssingment {
		if overrideCommit[topic] == nil {
			overrideCommit[topic] = map[int]int64{}

		}
		overrideCommit[topic][v.ID] = offset
	}
	err = gen.CommitOffsets(overrideCommit)

	if err != nil {
		return nil, err
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Topic:    topic,
		GroupID:  fmt.Sprintf("isaka-%s-%s", topic, groupID),
		Brokers:  k.brokersInfo.BrokerEndpoints(k.listener),
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
	return reader, nil
}

//refs: https://github.com/segmentio/kafka-go/issues/398
// thanks @robfig
func (k *KafkaBrokers) lastOffset(topic string) (int64, error) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var Dialer kafka.Dialer
	conn, err := k.dial(ctx, &Dialer)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		return 0, err
	}

	var lastOffsets = make([]int64, len(partitions))
	var group, ectx = errgroup.WithContext(ctx)
	for i, partition := range partitions {
		p := partition
		i := i
		group.Go(func() error {
			conn, err := Dialer.DialPartition(ectx, "tcp", "", p)
			if err != nil {
				return err
			}
			defer conn.Close()
			lastOffsets[i], err = conn.ReadLastOffset()
			if err != nil {
				return err
			}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return 0, err
	}
	return funk.MaxInt64(lastOffsets).(int64), nil
}

func (k *KafkaBrokers) dial(ctx context.Context, dialer *kafka.Dialer) (*kafka.Conn, error) {
	var (
		conn *kafka.Conn
		errs error
	)
	for _, broker := range k.brokersInfo.BrokerEndpoints(k.listener) {
		var err error
		conn, err = dialer.DialContext(ctx, "tcp", broker)
		if err != nil {
			errs = multierr.Append(errs, xerrors.Errorf("dialing %q: %w", broker, err))
			continue
		}
		return conn, nil
	}
	return nil, errs
}

type KafkaBrokerInfo struct {
	ListenerSecurityProtocolMap map[string]string `json:"listener_security_protocol_map"`
	RawEndpoints                []string          `json:"endpoints"`
	Endpoints                   map[string]string
	JmxPort                     string
	Host                        string
	Timestamp                   string
	Port                        int
	Version                     int
}

func (k *KafkaBrokerInfo) ParseEndpoint() {
	k.Endpoints = map[string]string{}
	for _, r := range k.RawEndpoints {
		for l, _ := range k.ListenerSecurityProtocolMap {
			protocol := fmt.Sprintf("%s://", l)
			if strings.Index(r, protocol) >= 0 {
				k.Endpoints[l] = r[len(protocol):]
			}
		}
	}
}

type KafkaBrokersInfo []KafkaBrokerInfo

func (ks KafkaBrokersInfo) BrokerEndpoints(listener string) []string {
	r := []string{}

	for _, k := range ks {
		v, ok := k.Endpoints[listener]
		if ok {
			r = append(r, v)
		}
	}
	return r
}
