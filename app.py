import pycurl, json, pymongo
from pymongo import Connection

STREAM_URL = "https://stream.twitter.com/1/statuses/sample.json"

USER = "timothyshi"
PASS = "071170"

class Client:
  def __init__(self):
    self.mongoConnection = Connection()
    self.db = self.mongoConnection.tweets
    self.collection = self.db.tweets
    self.buffer = ""
    self.conn = pycurl.Curl()
    self.conn.setopt(pycurl.USERPWD, "%s:%s" % (USER, PASS))
    self.conn.setopt(pycurl.URL, STREAM_URL)
    self.conn.setopt(pycurl.WRITEFUNCTION, self.on_receive)
    # self.conn.perform()
    


  def on_receive(self, data):
    self.buffer += data
    if data.endswith("\r\n") and self.buffer.strip():
      content = json.loads(self.buffer)
      self.buffer = ""
      try:
        tweet = {'id': content['id'],
                  'loc': content['geo']['coordinates'],
                  'text': content['text'],
                  'username': content['user']['screen_name'],
                  'created_at': content['created_at']}
        self.collection.insert(tweet)
        # print self.collection.count()
      except (KeyError, TypeError):
        pass

  def start_connection(self):
    self.conn.perform()

def main():
  client = Client()
  client.start_connection()

if __name__ == '__main__':
  main()