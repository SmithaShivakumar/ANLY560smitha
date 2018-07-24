import gdax, time
from pandas.io.json import json_normalize
import sqlite3
import json
from pprint import pprint
import pandas as pd
import numpy as np

class myWebsocketClient(gdax.WebsocketClient):
	def on_open(self):
		self.url = "wss://ws-feed.gdax.com/"
		self.products = ["BTC-USD", "ETH-USD"]
		self.msg_count = 0
		self.conn = sqlite3.connect("gdaxTradesData.db", check_same_thread=False)
		self.cur = self.conn.cursor()
	def on_message(self, msg):
		# self.cur.execute("delete from trades", self.conn)
		# self.cur.commit()
		try:
			if 'price' in msg and 'type' in msg and msg["type"] == 'received':
				self.msg_count += 1
				msg["cp_long"] = 'Bob'
				msg["cp_short"] = 'Dan'


				# self.cur.execute('CREATE TABLE new_testable(Trade_ID TEXT, Time DateTime, Product TEXT, Price REAL, Quantity REAL , CPLong TEXT, CPSHORT TEXT)')
				
				self.cur.execute("insert into new_testable (Trade_ID, Time, Product, Price, Quantity, CPLong, CPShort) values (?, ?, ?, ?, ?, ?, ?);", ('94f30de6-36b0-4c61-9f03-2ab5fe82d419', '2018-07-23T23:38:21.598000Z', 'BTC-USD', '7721.01000000', '1.00000000', 'Bob', 'Dan'))
					#(msg["order_id"], msg["time"], msg["product_id"], float(msg["price"]),float(msg["size"]),msg["cp_long"],msg["cp_short"]))
				# self.cur.commit()
				self.cur.execute("select * from new_testable", self.conn)
				self.conn.commit()
		except Exception as e:
			#print("Order ID",msg["order_id"],"Time", msg["time"],"Product ID", msg["product_id"],"Price", float(msg["price"]), "Size", float(msg["size"]),msg["cp_long"],msg["cp_short"])
			print("ERROR: ", e)
			print("\n", msg)

	def on_close(self):
		print("-- Goodbye! --")
		self.conn.close()

wsClient = myWebsocketClient()
wsClient.start()
print(wsClient.url, wsClient.products)
while (wsClient.msg_count < 10):
	print ("\nmessage_count =", "{} \n".format(wsClient.msg_count))
	time.sleep(1)
wsClient.close()



