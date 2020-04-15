package influxdb

import (
	uurl "net/url"
	"time"

	client "github.com/influxdata/influxdb1-client"
)

type InfClient struct {
	url      uurl.URL
	database string

	measurement string
	username    string
	password    string
	tags        map[string]string

	pts []client.Point

	client *client.Client
}

func NewClient(url, database, measurement, username, password string) (db *InfClient, err error) {
	u, err := uurl.Parse(url)
	if err != nil {
		return
	}

	inf, err := client.NewClient(client.Config{
		URL:      *u,
		Username: username,
		Password: password,
	})
	if err != nil {
		return
	}

	return &InfClient{
		url:         *u,
		database:    database,
		measurement: measurement,
		username:    username,
		password:    password,
		client:      inf,
	}, nil
}

func (db *InfClient) Push(measurement string, tags map[string]string, fields map[string]interface{}) (err error) {
	db.pts = append(db.pts, client.Point{
		Measurement: measurement,
		Tags:        tags,
		Fields:      fields,
		Time:        time.Now(),
	})

	return
}

func (db *InfClient) Writer() (err error) {
	bps := client.BatchPoints{
		Points:   db.pts,
		Database: db.database,
	}
	_, err = db.client.Write(bps)
	return
}
