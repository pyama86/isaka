/*
Copyright © 2020 NAME HERE <EMAIL ADDRESS>

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
	"time"

	"github.com/pyama86/isaka/isaka"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// tailCmd represents the tail command
var tailCmd = &cobra.Command{
	Use:   "tail",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		if err := runTail(); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	},
}

func runTail() error {
	z, err := isaka.NewZookeeper(config.ZookeeperHost, config.ClusterName, time.Duration(config.ZookeeperTimeout)*time.Second)
	if err != nil {
		return err
	}
	brokersInfo, err := z.BrokerList()
	if err != nil {
		return err
	}

	kafkaBrokers := isaka.NewKafkaBrokers(brokersInfo, config.Listener)
	reader, err := kafkaBrokers.Reader(config.Topic, os.Getenv("USER"), config.Tail)
	if err != nil {
		return err
	}

	defer reader.Close()
	var cnt int64
	for {
		m, err := reader.ReadMessage(context.Background())
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
	tailCmd.PersistentFlags().StringP("topic", "t", "", "topic")
	viper.BindPFlag("Topic", tailCmd.PersistentFlags().Lookup("topic"))

	tailCmd.PersistentFlags().Int64P("tail", "n", 20, "tail line")
	viper.BindPFlag("Tail", tailCmd.PersistentFlags().Lookup("tail"))

	tailCmd.PersistentFlags().BoolP("follow", "f", false, "follow input")
	viper.BindPFlag("Follow", tailCmd.PersistentFlags().Lookup("follow"))

	tailCmd.PersistentFlags().StringP("listener", "l", "PLAINTEXT", "choose listener")
	viper.BindPFlag("Listener", tailCmd.PersistentFlags().Lookup("listener"))
	rootCmd.AddCommand(tailCmd)

}
