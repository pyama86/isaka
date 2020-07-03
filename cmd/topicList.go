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
	"os"
	"strings"
	"time"

	"github.com/pyama86/isaka/isaka"
	"github.com/spf13/cobra"
)

// topicListCmd represents the topicList command
var topicListCmd = &cobra.Command{
	Use:   "topic-list",
	Short: "show topic list",
	Long:  `It is get topic list from zookeeper`,
	Run: func(cmd *cobra.Command, args []string) {
		if err := runTopicList(); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	},
}

func runTopicList() error {
	z, err := isaka.NewZookeeper(config.ZookeeperHost, config.ClusterName, time.Duration(config.ZookeeperTimeout)*time.Second)
	if err != nil {
		return err
	}
	topics, err := z.TopicList()
	if err != nil {
		return err
	}

	fmt.Fprint(os.Stdout, strings.Join(topics, "\n"))
	return nil

}
func init() {
	rootCmd.AddCommand(topicListCmd)
}
