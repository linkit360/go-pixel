package notifier

import (
	"encoding/json"
	"fmt"

	log "github.com/Sirupsen/logrus"

	"github.com/vostrok/utils/amqp"
)

type Pixel struct {
	Tid            string  `json:"tid,omitempty"`
	Msisdn         string  `json:"msisdn,omitempty"`
	CampaignId     int64   `json:"campaign_id,omitempty"`
	SubscriptionId int64   `json:"subscription_id,omitempty"`
	OperatorCode   int64   `json:"operator_code,omitempty"`
	CountryCode    int64   `json:"country_code,omitempty"`
	Pixel          string  `json:"pixel,omitempty"`
	Publisher      string  `json:"publisher,omitempty"`
	ResponseCode   int     `json:"response_code,omitempty"`
	Took           float64 `json:"took,omitempty"`
	Sent           bool    `json:"sent,omitempty"`
}

type Notifier interface {
	PixelNotify(msg Pixel) error
}

type NotifierConfig struct {
	Queue queues              `yaml:"queue"`
	Rbmq  amqp.NotifierConfig `yaml:"rbmq"`
}
type queues struct {
	PixelsQueue string `default:"pixels" yaml:"pixels"`
}

type notifier struct {
	q  queues
	mq *amqp.Notifier
}

type EventNotify struct {
	EventName string      `json:"event_name,omitempty"`
	EventData interface{} `json:"event_data,omitempty"`
}

func init() {
	log.SetLevel(log.DebugLevel)
}

func NewNotifierService(conf NotifierConfig) Notifier {
	var n Notifier
	{
		rabbit := amqp.NewNotifier(conf.Rbmq)
		n = &notifier{
			q: queues{
				PixelsQueue: conf.Queue.PixelsQueue,
			},
			mq: rabbit,
		}
	}
	return n
}

func (service notifier) PixelNotify(msg Pixel) error {
	log.WithField("pixel", fmt.Sprintf("%#v", msg)).Debug("got pixel")

	event := EventNotify{
		EventName: "pixels",
		EventData: msg,
	}
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("json.Marshal: %s", err.Error())
	}
	log.WithField("body", string(body)).Debug("sent body")
	service.mq.Publish(amqp.AMQPMessage{service.q.PixelsQueue, 0, body})
	return nil
}
