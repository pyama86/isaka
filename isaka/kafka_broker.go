package isaka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/thoas/go-funk"
	"go.uber.org/multierr"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
)

type KafkaBrokers struct {
	brokerEndpoints []string
	Timeout         int
	dialer          kafka.Dialer
}

func NewKafkaBrokers(brokerEndpoints []string, timeout int, ca, cert, key string) (*KafkaBrokers, error) {

	tlsConfig := new(tls.Config)
	if ca != "" {
		CA_Pool := x509.NewCertPool()

		severCert, err := ioutil.ReadFile(ca)
		if err != nil {
			return nil, err
		}
		CA_Pool.AppendCertsFromPEM(severCert)

		tlsConfig.RootCAs = CA_Pool
	}

	if cert != "" && key != "" {
		x509Cert, err := tls.LoadX509KeyPair(cert, key)
		if err != nil {
			return nil, err
		}
		tlsConfig.Certificates = make([]tls.Certificate, 1)
		tlsConfig.Certificates[0] = x509Cert
	}

	if len(tlsConfig.Certificates) == 0 && tlsConfig.RootCAs == nil {
		tlsConfig = nil
	}

	dialer := kafka.Dialer{
		Timeout:   time.Duration(timeout) * time.Second,
		DualStack: true,
		TLS:       tlsConfig,
	}

	return &KafkaBrokers{
		brokerEndpoints: brokerEndpoints,
		dialer:          dialer,
		Timeout:         timeout,
	}, nil
}

func (k *KafkaBrokers) Reader(topic, groupID string, tail int64) (*kafka.Reader, error) {
	o, err := k.lastOffset(topic)
	if err != nil {
		return nil, err
	}

	offset := o - tail - 1

	group, err := kafka.NewConsumerGroup(kafka.ConsumerGroupConfig{
		ID:      fmt.Sprintf("isaka-%s-%s", topic, groupID),
		Brokers: k.brokerEndpoints,
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
		Brokers:  k.brokerEndpoints,
		MinBytes: 1e3,  // 1KB
		MaxBytes: 10e6, // 10MB
		Dialer:   &k.dialer,
	})
	return reader, nil
}

//refs: https://github.com/segmentio/kafka-go/issues/398
// thanks @robfig
func (k *KafkaBrokers) lastOffset(topic string) (int64, error) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Duration(k.Timeout)*time.Second)
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
	for _, broker := range k.brokerEndpoints {
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
