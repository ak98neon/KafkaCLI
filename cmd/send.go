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
	"github.com/Shopify/sarama"
	"github.com/ak98neon/kafka_cli/kafka"
	"github.com/spf13/cobra"
	"io/ioutil"
	"log"
	"math/rand"
	"strconv"
	"time"
)

// sendCmd represents the send command
var sendCmd = &cobra.Command{
	Use:   "send",
	Short: "Produce message or file to kafka topic",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		broker, _ := cmd.Flags().GetString("broker")
		kafka.BrokerList = []string{broker}

		clientName, _ := cmd.Flags().GetString("client")
		kafka.ClientId = clientName

		topic, _ := cmd.Flags().GetString("topic")
		count, _ := cmd.Flags().GetInt("count")

		filePath, _ := cmd.Flags().GetString("file")
		msg, _ := cmd.Flags().GetString("msg")
		dirPath, _ := cmd.Flags().GetString("dir")

		if filePath != "" {
			file, err := ioutil.ReadFile(filePath)
			if err != nil {
				log.Panic(err)
			}
			sendMessage(topic, string(file), count)
		} else if msg != "" {
			sendMessage(topic, msg, count)
		} else if dirPath != "" {
			dir, err := ioutil.ReadDir(dirPath)
			if err != nil {
				log.Panic(err)
			}

			for _, f := range dir {
				if !f.IsDir() {
					file, _ := ioutil.ReadFile(f.Name())
					sendMessage(topic, string(file), count)
				}
			}
		} else {
			panic("Message or File Path must be set!")
		}
	},
}

func sendMessage(topic, value string, count int) {
	producerMessage := sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(value),
		Headers: []sarama.RecordHeader{
			{
				Key:   []byte("ORIG-TIME"),
				Value: []byte(strconv.Itoa(int(time.Now().Unix()))),
			},
			{
				Key:   []byte("ORIG-TTL"),
				Value: []byte(strconv.Itoa(rand.Int())),
			},
			{
				Key:   []byte("TRACE-ID"),
				Value: []byte(strconv.Itoa(rand.Int())),
			},
		},
		Timestamp: time.Time{},
	}
	kafka.ProduceMessage(&producerMessage, count, topic)
}

func init() {
	sendCmd.Flags().StringP("file", "f", "", "Set path to file, that need be send")
	sendCmd.Flags().StringP("dir", "d", "", "Send all files from directory")
	sendCmd.Flags().StringP("msg", "s", "", "Set message that will be send")

	sendCmd.Flags().IntP("count", "c", 1, "Count of message that will be send")

	sendCmd.Flags().StringP("broker", "b", "localhost:5432", "Set kafka broker")
	sendCmd.Flags().StringP("topic", "t", "", "Topic name")
	sendCmd.Flags().StringP("client", "l", "client", "Client name")

	_ = sendCmd.MarkFlagRequired("topic")
	_ = sendCmd.MarkFlagRequired("broker")
	rootCmd.AddCommand(sendCmd)
}
