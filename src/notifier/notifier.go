package notifier

import (
	"encoding/json"
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/linkit360/go-utils/amqp"
)

type Pixel struct {
	Tid            string    `json:"tid,omitempty"`
	Msisdn         string    `json:"msisdn,omitempty"`
	CampaignId     int64     `json:"campaign_id,omitempty"`
	ServiceId      int64     `json:"service_id,omitempty"`
	SubscriptionId int64     `json:"subscription_id,omitempty"`
	OperatorCode   int64     `json:"operator_code,omitempty"`
	CountryCode    int64     `json:"country_code,omitempty"`
	Pixel          string    `json:"pixel,omitempty"`
	Publisher      string    `json:"publisher,omitempty"`
	Endpoint       string    `json:"endpoint,omitempty"`
	ResponseCode   int       `json:"response_code,omitempty"`
	Took           float64   `json:"took,omitempty"`
	Sent           bool      `json:"sent,omitempty"`
	SentAt         time.Time `json:"sent_at,omitempty"`
}

type Notifier interface {
	PixelNotify(msg Pixel) error
	PixelTransactionNotify(msg Pixel) error
	PixelUpdateSubscriptionNotify(msg Pixel) error
	PixelRemoveBufferedNotify(msg Pixel) error
}

type NotifierConfig struct {
	Queue queues              `yaml:"queue"`
	Rbmq  amqp.NotifierConfig `yaml:"rbmq"`
}
type queues struct {
	PixelsQueue    string `default:"pixels" yaml:"pixels"`
	PixelSentQueue string `default:"pixel_sent" yaml:"pixel_sent"`
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
			q:  conf.Queue,
			mq: rabbit,
		}
	}
	return n
}

func (service notifier) PixelNotify(msg Pixel) error {
	msg.SentAt = time.Now().UTC()
	event := EventNotify{
		EventName: "pixels",
		EventData: msg,
	}
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("json.Marshal: %s", err.Error())
	}
	log.WithField("body", string(body)).Debug("sent body")
	service.mq.Publish(amqp.AMQPMessage{service.q.PixelsQueue, 0, body, event.EventName})
	return nil
}

func (service notifier) PixelTransactionNotify(msg Pixel) error {
	msg.SentAt = time.Now().UTC()
	event := EventNotify{
		EventName: "transaction",
		EventData: msg,
	}
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("json.Marshal: %s", err.Error())
	}
	service.mq.Publish(amqp.AMQPMessage{service.q.PixelSentQueue, 0, body, event.EventName})
	return nil
}

func (service notifier) PixelUpdateSubscriptionNotify(msg Pixel) error {
	msg.SentAt = time.Now().UTC()
	event := EventNotify{
		EventName: "update",
		EventData: msg,
	}
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("json.Marshal: %s", err.Error())
	}
	service.mq.Publish(amqp.AMQPMessage{service.q.PixelSentQueue, 0, body, event.EventName})
	return nil
}

func (service notifier) PixelRemoveBufferedNotify(msg Pixel) error {
	msg.SentAt = time.Now().UTC()
	event := EventNotify{
		EventName: "remove_buffered",
		EventData: msg,
	}
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("json.Marshal: %s", err.Error())
	}
	service.mq.Publish(amqp.AMQPMessage{service.q.PixelSentQueue, 1, body, event.EventName})
	return nil
}
