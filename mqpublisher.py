# -*- coding: utf-8 -*-
# vim: ts=4:sw=4:expandtab
"""

    RabbitMQ 库封装，主要是针对多台rabbitmq服务器的HA支持, 另外对执行动作时的心跳检测。
    
"""

import os
import pika
import time
from functools import wraps

import logging
from logging import handlers
logger = logging.getLogger('buildplatform-mq')
logger.setLevel(logging.DEBUG)
BASE_DIR = os.path.realpath(os.path.join(os.path.dirname(__file__), '../'))
log_file = os.path.join(BASE_DIR, 'logs', 'buildplatform-mq.log')
fh = handlers.TimedRotatingFileHandler(log_file, when='D', interval=1, backupCount=3)
fh.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
logger.addHandler(fh)
logger.addHandler(ch)


class BuildPlatformRabbit:
    """
    BuildPlatform rabbitmq 发布消息封装，支持rabbitmq服务器列表
    """
    def __init__(self, mq_host=[], mq_user='user', mq_pass='passwd'):
        # FIXME: 用户名密码以及rabbitmq服务器列表需要藏匿,使用ConfigCenter来配置
        demo_hosts = [
            'rabbit01',
            'rabbit02'
        ]
        self.servers = mq_host if mq_host else demo_hosts
        self.server_index = 0
        self.credentials = pika.PlainCredentials(mq_user, mq_pass)
        self.connection = None
        self.channel = None
        self.last_process_event_call = time.time()

    def keep_connect_rabbit(fn):
        """
        rabbitmq连接装饰器, RabbitMQ连接时需要用该装饰器
        :param fn:
        :return:
        """
        def wrapper(cls, *args, **kwargs):
            cls.server_index = 0
            ret = fn(cls, *args, **kwargs)
            while not ret:
                cls.server_index += 1
                if cls.server_index > len(cls.servers)-1:
                    logger.warning('It is end of rabbitmq server.')
                    ret = None
                    break
                ret = fn(cls, *args, **kwargs)
            return ret

        return wrapper

    def heart_beat(fn):
        """
        rabbitmq 连接心跳检测, 所有执行RabbitMQ操作的动作需要加该装饰器
        :param fn:
        :return:
        """
        def wrapper(cls, *args, **kwargs):
            if not cls.connection or not cls.connection.is_open:
                connection = cls.conn()
                if not connection:
                    logger.error('Can not connect to the rabbitmq server, abort')
                    return False

            return fn(cls, *args, **kwargs)

        return wrapper

    @keep_connect_rabbit
    def conn(self):
        """
        连接 RabbitMQ 服务器
        :return:
        """
        try:
            rabbitmq_server = self.servers[self.server_index]
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=rabbitmq_server, credentials=self.credentials)
            )
            logger.info('connect to {server}'.format(server=rabbitmq_server))
            self.connection = connection
            return connection
        except pika.exceptions.ProbableAuthenticationError:
            log_msg = 'credentials {user}:{passwd} is not match for {server}'.format(
                user=self.credentials.username, passwd=self.credentials.password, server=rabbitmq_server
            )
            logger.error(log_msg)
        except pika.exceptions.AMQPConnectionError:
            log_msg = 'connect to {server} occour error'.format(server=rabbitmq_server)
            logger.error(log_msg)

        return None

    def close(self):
        """
        关闭 RabbitMQ 连接
        :return:
        """
        if self.connection:
            self.connection.close()

    @heart_beat
    def get_channel(self):
        """
        获得RabbitMQ的channel对象
        :return: Rabbintmq channel object
        """
        channel = self.connection.channel()
        return channel

    @heart_beat
    def publish(self, msg_body='demo', exchange='Cbuilder-Ex', routing_key='', priority=5):
        """
        发布消息，使用confirm_delivery模式
        :param msg_body: 消息主体
        :param exchange: 交换机名称
        :param routing_key: 路由键名称
        :param priority: 该消息在队列中的优先级定义，可接受范围0~10, 数字越大优先级越高.
        :return:
        """
        channel = self.connection.channel()
        channel.confirm_delivery()

        # 如果exchange非空，则申明exchange
        if exchange:
            channel.exchange_declare(exchange=exchange, type='direct', durable=True)
        else:
            # 如果exchange为空，queue名称为routing_key值
            arguments = {
                'x-max-priority': 10,
                'ha-mode': 'all'
            }
            channel.queue_declare(queue=routing_key, arguments=arguments, durable=True)

        try:
            properties = pika.BasicProperties(priority=priority, content_type='text/plain', delivery_mode=1)
            channel.basic_publish(exchange=exchange, routing_key=routing_key, body=msg_body,
                                  properties=properties)
            logger.info("Sent to exchange done. ")
            return True
        except pika.exceptions.ChannelClosed, e:
            logger.error(e[1])
        return False

    def event_call(self):
        """
        给rabbitmq发送心跳包，publish业务逻辑中需要加入此功能
        :return: 发送返回True， 不做动作返回False
        """
        if time.time() - self.last_process_event_call > 60:
            try:
                self.connection.process_data_events()
            except AttributeError:
                self.conn()
            self.last_process_event_call = time.time()
            logger.debug('sent process event call')
            return True

        return False

    @staticmethod
    def callback(fn):
        """
        RabbitMQ 消费进程的回调方法神奇封装, 如自定义的回调处理过程无异常，则自动ACK, 让自定义回调处理只关注处理本身
        使用范例:
            class demo:
                @BuildPlatformRabbit.callback
                def __call__(self, *args, **kwargs):
                    channel, method, properties, body = args
                    # Do something here

            d = demo()
            channel.basic_consume(consumer_callback=d)

        :param fn:
        :return:
        """
        @wraps(fn)
        def magic_callback(cls, channel, method, properties, body):
            logger.info("routing key is %r %r" % (method.routing_key, properties))
            try:
                fn(cls, body)
            except Exception, e:
                # TODO: 移除traceback部分
                import traceback
                traceback.print_exc()
                logger.error('Exception: %r, this msg can not ack' % (e))

                # 驳回队列消息的ACK
                channel.basic_nack(delivery_tag=method.delivery_tag)
                return False

            # ACK 队列消息
            channel.basic_ack(delivery_tag=method.delivery_tag)

            return True

        return magic_callback

    @heart_beat
    def receiver(self, queue_name='Cbuilder-Event', callback=None):
        """
        接受者
        :param queue_name 需要消费的队列名称
        :param callback callback功能方法
        :return:
        """
        self.channel = self.connection.channel()
        self.channel.basic_qos(prefetch_count=1)

        logger.debug('Waiting for logs. To exit press CTRL+C')
        if not callback:
            callback = self.callback
        self.channel.basic_consume(callback, queue=queue_name, no_ack=False)

        try:
            self.channel.start_consuming()
        except pika.exceptions.ConnectionClosed:
            logger.error('Connection closed')
        except KeyboardInterrupt:
            logger.warning('User Interputer, exit.')
        finally:
            self.channel.stop_consuming()


class demo():
    @BuildPlatformRabbit.callback
    def __call__(self, *args, **kwargs):
        print 'demo'

if __name__ == '__main__':
    bp = BuildPlatformRabbit()
    d = demo()
    bp.receiver(callback=d, queue_name='bp-net-test')
