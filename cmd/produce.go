/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kgo"
)

// publishCmd represents the publish command
var publishCmd = &cobra.Command{
	Use:   "produce",
	Short: "produce message to kafka",
	Long: `Produce message to kafka via command 
	Example:
	app produce --brokers 127.0.0.1:19092 --topic FooTopic  --json '{"topic":"topic_name","message":"message"}'

	Example using file:
	app produce --brokers 127.0.0.1:19092 --topic FooTopic  --jsonfile data/sample1.json
	`,
	Run: publishMessage,
}

func init() {
	rootCmd.AddCommand(publishCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// publishCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// publishCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

	publishCmd.Flags().StringP("json", "j", "", "json message")
	publishCmd.Flags().StringP("jsonfile", "f", "", "load json file")
	publishCmd.Flags().StringP("topic", "t", "", "topic name")
	publishCmd.Flags().StringP("brokers", "b", "", "brokers")

}

func publishMessage(cmd *cobra.Command, args []string) {

	brokers := cmd.Flag("brokers").Value.String()
	if brokers == "" {
		fmt.Println("brokers is required")
		return
	}

	kgoOpts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
	}

	broker, err := kgo.NewClient(kgoOpts...)
	if err != nil {
		fmt.Println("failed to create broker")
		return
	}

	dataMap := []map[string]interface{}{}

	jsonMessage := cmd.Flag("json").Value.String()
	if jsonMessage == "" {
		jsonFilePath := cmd.Flag("jsonfile").Value.String()
		if jsonFilePath != "" {
			dat, err := os.ReadFile(jsonFilePath)
			if err != nil {
				fmt.Println("failed to read file")
				return
			}
			jsonMessage = string(dat)
		}
	}

	if jsonMessage != "" {
		if err := json.Unmarshal([]byte(jsonMessage), &dataMap); err != nil {
			fmt.Println("failed to unmarshal json")
			return
		}
	}

	for i, item := range dataMap {
		jsonOut, err := json.Marshal(item)
		if err != nil {
			fmt.Println("failed to marshal json")
			return
		}

		broker.ProduceSync(context.Background(), &kgo.Record{
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Topic: cmd.Flag("topic").Value.String(),
			Value: jsonOut,
		})

		fmt.Println(string(jsonOut))
	}

}
