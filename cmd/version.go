package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var version string

func init() {
	rootCmd.AddCommand(versionCmd)
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version number of Isaka",
	Long:  `All software has versions. This is Isaka's`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("Isaka v%s\n", version)
	},
}
