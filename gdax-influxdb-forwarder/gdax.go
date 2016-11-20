package main

import (
	"fmt"
	influxdb "github.com/influxdata/influxdb/client/v2"
	exchange "github.com/preichenberger/go-coinbase-exchange"
	"os"
	"time"
)

type EnvVars struct {
	secret       string
	key          string
	passphrase   string
	influxdbIP   string
	influxdbPort string
}

func main() {

	var ev EnvVars

	if ev.secret = os.Getenv("GDAX_SECRET"); len(ev.secret) == 0 {
		fmt.Println("GDAX_SECRET environment variable not set")
		os.Exit(1)
	}

	if ev.key = os.Getenv("GDAX_KEY"); len(ev.key) == 0 {
		fmt.Println("GDAX_KEY environment variable not set")
		os.Exit(1)
	}

	if ev.passphrase = os.Getenv("GDAX_PASSPHRASE"); len(ev.passphrase) == 0 {
		fmt.Println("GDAX_PASSPHRASE environment variable not set")
		os.Exit(1)
	}

	if ev.influxdbIP = os.Getenv("INFLUXDB_IP"); len(ev.influxdbIP) == 0 {
		fmt.Println("INFLUXDB_IP environment variable not set")
		os.Exit(1)
	} else {
		fmt.Println(ev.influxdbIP)
	}

	if ev.influxdbPort = os.Getenv("INFLUXDB_PORT"); len(ev.influxdbPort) == 0 {
		fmt.Println("INFLUXDB_PORT environment variable not set")
		os.Exit(1)
	}

	fmt.Println(ev)

	gdaxClient := exchange.NewClient(ev.secret, ev.key, ev.passphrase)

	influxClient, err := influxdb.NewHTTPClient(influxdb.HTTPConfig{
		Addr:    fmt.Sprintf("http://%s:%s", ev.influxdbIP, ev.influxdbPort),
		Timeout: time.Duration(3 * time.Second),
	})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	defer influxClient.Close()

	for {
		if _, _, err := influxClient.Ping(time.Duration(3 * time.Second)); err != nil {
			fmt.Println("Influxdb Ping failed server is unavailable")
			time.Sleep(time.Duration(3 * time.Second))
			continue
		} else {
			break
		}
	}

	_, err = influxClient.Query(influxdb.NewQuery("CREATE DATABASE BITCOIN", "", ""))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
	}

	exit := make(chan int)
	candle := make(chan []exchange.HistoricRate, 100)
	period := time.Tick(1 * time.Minute)

	go writeCandleToInfluxDB(candle, influxClient)

	go func() {
		for _ = range period {
			sticks, err := gdaxCurrentCandleStick(gdaxClient)
			if err != nil {
				continue
			}
			candle <- sticks
		}
	}()

	<-exit
}

func writeCandleToInfluxDB(c <-chan []exchange.HistoricRate, i influxdb.Client) {
	for candleSticks := range c {

		bp, err := influxdb.NewBatchPoints(influxdb.BatchPointsConfig{
			Database:  "BITCOIN",
			Precision: "s",
		})
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			continue
		}

		tags := map[string]string{"exchange": "gdax"}
		fields := map[string]interface{}{
			"low":    candleSticks[0].Low,
			"high":   candleSticks[0].High,
			"open":   candleSticks[0].Open,
			"close":  candleSticks[0].Close,
			"volume": candleSticks[0].Volume,
		}

		pt, err := influxdb.NewPoint("btc", tags, fields, candleSticks[0].Time)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			continue
		}

		bp.AddPoint(pt)
		err = i.Write(bp)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			continue
		}

	}
}

func gdaxCurrentCandleStick(e *exchange.Client) ([]exchange.HistoricRate, error) {

	candleSticks, err := e.GetHistoricRates("BTC-USD", exchange.GetHistoricRatesParams{
		Start:       time.Unix(time.Now().Unix()-int64(60), 0),
		End:         time.Now(),
		Granularity: 60,
	})

	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return []exchange.HistoricRate{}, err
	}

	fmt.Println(candleSticks[0])
	return candleSticks, nil

}
