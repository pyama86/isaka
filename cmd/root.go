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
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/spf13/cobra"

	"github.com/spf13/viper"
)

type Config struct {
	ZookeeperHost    string
	ClusterName      string
	Topic            string
	Listener         string
	ZookeeperTimeout int
	KafkaTimeout     int
	Tail             int64
	Follow           bool
	BrokerCA         string
	BrokerCert       string
	BrokerKey        string
	KafkaBrokers     string
}

var config Config

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "isaka",
	Short: "A brief description of your application",
	Long: `A longer description that spans multiple lines and likely contains
examples and usage of using your application. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	//	Run: func(cmd *cobra.Command, args []string) { },
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func init() {
	viper.SetEnvPrefix("Isaka")

	rootCmd.PersistentFlags().StringP("zookeeper", "z", "", "zookeeper host(Env:ISAKA_ZOOKEEPERHOST)")
	viper.BindPFlag("ZookeeperHost", rootCmd.PersistentFlags().Lookup("zookeeper"))

	rootCmd.PersistentFlags().Int("zookeeper-timeout", 10, "zookeeper timeout")
	viper.BindPFlag("ZookeeperTimeout", rootCmd.PersistentFlags().Lookup("zookeeper-timeout"))

	rootCmd.PersistentFlags().StringP("cluster", "c", "cluster", "cluster name(Env:ISAKA_CLUSTERNAME)")
	viper.BindPFlag("ClusterName", rootCmd.PersistentFlags().Lookup("cluster"))

	cobra.OnInitialize(func() {
		viper.AutomaticEnv()
		if err := viper.Unmarshal(&config); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	})
	log.SetOutput(ioutil.Discard)

}
