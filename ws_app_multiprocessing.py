import datetime
import time
from multiprocessing import cpu_count
import tornado.concurrent
import tornado.escape
import tornado.httpserver
import tornado.ioloop
import tornado.web
import tornado.websocket
from lib.aioprocessing.aioprocessing import AioQueue
from lib.redis_set_value_proc import SetRedisValue

set_redis_queue = AioQueue()

class WebSocketHandler(tornado.websocket.WebSocketHandler):
    def open(self):
        self.timeout = tornado.ioloop.IOLoop.instance().add_timeout(datetime.timedelta(seconds=5), self.send_ping)

    def check_origin(self, origin):
        return True

    def on_message(self, message):
        set_redis_queue.put(message)

    def on_close(self):
        tornado.ioloop.IOLoop.instance().remove_timeout(self.timeout)
        print("WS closed")

    def send_ping(self):
        if self:
            ts = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            self.write_message(ts)
            self.timeout = tornado.ioloop.IOLoop.instance().add_timeout(datetime.timedelta(seconds=5), self.send_ping)

class Application(tornado.web.Application):
    def __init__(self):
        handlers = [('/ws', WebSocketHandler)]
        tornado.web.Application.__init__(self, handlers=handlers, debug=True)

def launch_processes():
    redis_set_values_process_list = []
    for _ in range(int(cpu_count())):
        add_to_redis_set_values_proc = SetRedisValue(set_redis_queue)
        add_to_redis_set_values_proc.daemon = True
        add_to_redis_set_values_proc.start()
        redis_set_values_process_list.append(add_to_redis_set_values_proc)

if __name__ == "__main__":
    launch_processes()
    ws_app = Application()
    server = tornado.httpserver.HTTPServer(ws_app)
    server.listen(port=8080)
    tornado.ioloop.IOLoop.instance().start()
