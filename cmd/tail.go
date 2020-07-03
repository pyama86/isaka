/*
Copyright © 2020 @pyama86 www.kazu.com@gmail.com

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/pyama86/isaka/isaka"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// tailCmd represents the tail command
var tailCmd = &cobra.Command{
	Use:   "tail",
	Short: "tail log from choose topic",
	Long: `it is tail command for Apache kafka.
You can set Broker list from Zookeeper or CLI options.`,
	Run: func(cmd *cobra.Command, args []string) {
		if err := runTail(); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	},
}

func runTail() error {
	brokerEndpoints := []string{}

	if config.KafkaBrokers == "" {

		z, err := isaka.NewZookeeper(config.ZookeeperHost, config.ClusterName, time.Duration(config.ZookeeperTimeout)*time.Second)
		if err != nil {
			return err
		}
		brokersInfo, err := z.BrokerList()
		if err != nil {
			return err
		}

		brokerEndpoints = brokersInfo.BrokerEndpoints(config.Listener)
	} else {
		brokerEndpoints = strings.Split(config.KafkaBrokers, ",")
	}

	kafkaBrokers, err := isaka.NewKafkaBrokers(brokerEndpoints, config.KafkaTimeout, config.BrokerCA, config.BrokerCert, config.BrokerKey)
	if err != nil {
		return err
	}

	reader, err := kafkaBrokers.Reader(config.Topic, os.Getenv("USER"), config.Tail)
	if err != nil {
		return err
	}

	defer reader.Close()
	var cnt int64

	ctx := context.Background()

	for {
		ctx, cancel := context.WithTimeout(ctx, time.Duration(config.KafkaTimeout)*time.Second)
		defer cancel()

		m, err := reader.ReadMessage(ctx)
		if err != nil {
			return err
		}
		fmt.Fprintln(os.Stdout, string(m.Value))

		if !config.Follow {
			cnt++
			if cnt >= config.Tail {
				break
			}
		}
	}

	return nil

}

func init() {
	tailCmd.PersistentFlags().String("broker-ca", "", "ca cert file(Env:ISAKA_BROKERCA)")
	viper.BindPFlag("BrokerCA", tailCmd.PersistentFlags().Lookup("broker-ca"))

	tailCmd.PersistentFlags().String("broker-tls-cert", "", "tls cert file(Env:ISAKA_BROKERCERT)")
	viper.BindPFlag("BrokerCert", tailCmd.PersistentFlags().Lookup("broker-tls-cert"))

	tailCmd.PersistentFlags().String("broker-tls-key", "", "tls key file(Env:ISAKA_BROKERKEY)")
	viper.BindPFlag("BrokerKey", tailCmd.PersistentFlags().Lookup("broker-tls-key"))

	tailCmd.PersistentFlags().String("kafka-brokers", "", "kafka brokers")
	viper.BindPFlag("KafkaBrokers", tailCmd.PersistentFlags().Lookup("kafka-brokers"))

	tailCmd.PersistentFlags().StringP("topic", "t", "", "subscribe topic")
	viper.BindPFlag("Topic", tailCmd.PersistentFlags().Lookup("topic"))

	tailCmd.PersistentFlags().Int64P("tail", "n", 20, "tail line")
	viper.BindPFlag("Tail", tailCmd.PersistentFlags().Lookup("tail"))

	tailCmd.PersistentFlags().BoolP("follow", "f", false, "follow input")
	viper.BindPFlag("Follow", tailCmd.PersistentFlags().Lookup("follow"))

	tailCmd.PersistentFlags().StringP("listener", "l", "PLAINTEXT", "choose listener")
	viper.BindPFlag("Listener", tailCmd.PersistentFlags().Lookup("listener"))

	rootCmd.PersistentFlags().Int("kafka-timeout", 10, "kafka timeout")
	viper.BindPFlag("KafkaTimeout", rootCmd.PersistentFlags().Lookup("kafka-timeout"))

	rootCmd.AddCommand(tailCmd)

}
