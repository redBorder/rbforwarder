package listeners

import (
	"time"

	"github.com/redBorder/rb-forwarder/util"

	"github.com/Sirupsen/logrus"
	kafkaClient "github.com/stealthly/go_kafka_client"
)

var kafkaLog *logrus.Entry
var consumers []*kafkaClient.Consumer

type KafkaListener struct {
	rawConfig   util.Config
	c           chan *util.Message
	messagePool *util.MessagePool
	topics      []string

	counter int64

	config *kafkaClient.ConsumerConfig
}

func (l *KafkaListener) Listen(messagePool *util.MessagePool) chan *util.Message {
	kafkaLog = util.NewLogger("kafka-listener")
	kafkaClient.Logger = kafkaClient.NewDefaultLogger(kafkaClient.ErrorLevel)

	// Create the message channel
	l.c = make(chan *util.Message)
	l.messagePool = messagePool

	// Parse the configuration
	l.parseConfig()

	numConsumers := len(l.topics)
	consumers := make([]*kafkaClient.Consumer, numConsumers)
	for i := 0; i < numConsumers; i++ {
		config := *l.config
		config.Strategy = l.GetStrategy()
		config.WorkerFailureCallback = FailedCallback
		config.WorkerFailedAttemptCallback = FailedAttemptCallback
		consumers[0] = kafkaClient.NewConsumer(&config)
		topics := map[string]int{l.topics[0]: config.NumConsumerFetchers}
		go func() {
			consumers[0].StartStatic(topics)
		}()
	}

	// // Start timer for show messages per second
	// go func() {
	// 	for {
	// 		timer := time.NewTimer(1 * time.Second)
	// 		<-timer.C
	// 		kafkaLog.Debugf("Fetching %d/s", l.counter)
	// 		l.counter = 0
	// 	}
	// }()

	return l.c
}

func (l *KafkaListener) GetStrategy() func(*kafkaClient.Worker, *kafkaClient.Message, kafkaClient.TaskId) kafkaClient.WorkerResult {
	return func(_ *kafkaClient.Worker, msg *kafkaClient.Message, id kafkaClient.TaskId) kafkaClient.WorkerResult {
		message := l.messagePool.Take()
		message.Attributes["path"] = msg.Topic
		message.InputBuffer.Write(msg.Value)
		l.counter++
		l.c <- message
		return kafkaClient.NewSuccessfulResult(id)
	}
}

func (l *KafkaListener) Close() {
	kafkaLog.Infof("Shutdown triggered, closing all alive consumers")
	for _, consumer := range consumers {
		<-consumer.Close()
	}

	kafkaLog.Infof("Successfully shut down all consumers")
}

func FailedCallback(wm *kafkaClient.WorkerManager) kafkaClient.FailedDecision {
	return kafkaClient.DoNotCommitOffsetAndStop
}

func FailedAttemptCallback(task *kafkaClient.Task, result kafkaClient.WorkerResult) kafkaClient.FailedDecision {
	return kafkaClient.CommitOffsetAndContinue
}

// parseConfig
func (l *KafkaListener) parseConfig() {

	var brokers []string

	if l.rawConfig["topics"] != nil {
		topicsAux := l.rawConfig["topics"].([]interface{})
		for i := 0; i < len(topicsAux); i++ {
			l.topics = append(l.topics, topicsAux[i].(string))
		}
	}

	if l.rawConfig["zookeeper"] != nil {
		brokersAux := l.rawConfig["zookeeper"].([]interface{})
		for i := 0; i < len(brokersAux); i++ {
			brokers = append(brokers, brokersAux[i].(string))
			log.Print("Added broker " + brokersAux[i].(string))
		}
	}

	l.config = kafkaClient.DefaultConsumerConfig()
	l.config.Groupid = "go-pipes"

	// Worker
	l.config.NumWorkers = 1
	l.config.MaxWorkerRetries = 3
	l.config.WorkerBackoff = 500 * time.Millisecond
	l.config.WorkerRetryThreshold = int32(100)
	l.config.WorkerThresholdTimeWindow = 500 * time.Millisecond
	l.config.WorkerTaskTimeout = 1 * time.Minute
	l.config.WorkerManagersStopTimeout = 1 * time.Minute

	// Rebalance settings
	l.config.BarrierTimeout = 5 * time.Second
	l.config.RebalanceMaxRetries = int32(3)
	l.config.RebalanceBackoff = 5 * time.Second
	l.config.PartitionAssignmentStrategy = "range"
	l.config.ExcludeInternalTopics = true

	// Fetcher settings
	l.config.NumConsumerFetchers = 1
	if l.rawConfig["batchsize"] != nil {
		l.config.FetchBatchSize = l.rawConfig["batchsize"].(int)
	} else {
		l.config.FetchBatchSize = 1
	}
	l.config.FetchBatchTimeout = 1 * time.Second
	l.config.RequeueAskNextBackoff = 1 * time.Second
	l.config.SocketTimeout = 30 * time.Second
	l.config.QueuedMaxMessages = 1000
	l.config.RefreshLeaderBackoff = 200 * time.Millisecond
	l.config.FetchTopicMetadataRetries = 3
	l.config.FetchTopicMetadataBackoff = 500 * time.Millisecond
	// l.config.FetchMinBytes = int32(1)
	l.config.FetchWaitMaxMs = 500
	l.config.FetchMessageMaxBytes = int32(100000000)

	// Offset
	zkConfig := kafkaClient.NewZookeeperConfig()
	zkConfig.ZookeeperConnect = brokers
	zkConfig.ZookeeperTimeout = 5 * time.Second
	l.config.Coordinator = kafkaClient.NewZookeeperCoordinator(zkConfig)
	l.config.AutoOffsetReset = "smallest"
	l.config.OffsetsCommitMaxRetries = 5
	l.config.OffsetCommitInterval = 10 * time.Second

	l.config.DeploymentTimeout = 0 * time.Second
}
