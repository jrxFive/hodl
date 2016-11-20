from bitstampy import api
from influxdb import InfluxDBClient
import time
import schedule
import Queue
import threading


def writeCandleStickToInflux(influx_client, tickerQueue):
    if not tickerQueue.empty():
        candleStick = tickerQueue.get()

        points_influx_body = [{
            "measurement": "btc",
            "tags": {
                "exchange":"bitstamp"
            },
            "fields": {
                "low":    candleStick["low"],
                "high":   candleStick["high"],
                "open":   candleStick["open"],
                "close":  candleStick["last"],
                "volume": candleStick["volume"],
            },
            "time": candleStick["timestamp"]
        }]

        print(points_influx_body)

        influx_client.write_points(points_influx_body)
    else:
        print("ticker queue is empty")

def getCandleStick(tickerQueue):
    try:
        t = api.ticker()
        tickerQueue.put(t)
    except:
        return []

def run_threaded(func,*args,**kwargs):
    t = threading.Thread(target=func,args=args,kwargs=kwargs)
    t.start()


if __name__ == "__main__":
    influx_client = InfluxDBClient(host='influxdb',
                                   port=8086,
                                   username="root",
                                   password="root",
                                   database='BITCOIN',
                                   timeout=3)

    influx_client.create_database("BITCOIN")

    tickerQueue = Queue.Queue()

    schedule.every(60).seconds.do(run_threaded,
                                 getCandleStick,
                                 tickerQueue)

    schedule.every(5).seconds.do(run_threaded,
                                writeCandleStickToInflux,
                                influx_client,
                                tickerQueue)


    while True:
        schedule.run_pending()
        time.sleep(1)