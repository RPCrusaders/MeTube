import pika
import sys
import json


connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

def _consume_youtuber_requests(ch, method, properties, body):
    body = json.loads(body)
    print(f"{body['youtuber']} uploaded {body['video_name']}")
    # print(body)

def _consume_user_requests(ch, method, properties, body):
    body = json.loads(body)
    print(f"{body['user']} subscribed {body['youtuber']}")
    # print(body)

def main():
    channel.exchange_declare(exchange="content", exchange_type="direct", durable=True)
    channel.exchange_declare(exchange="server_info", exchange_type="fanout", durable=True)
    result = channel.queue_declare(queue="llo", durable=True)

    # binding the queue to the exchange
    channel.queue_bind(exchange="server_info", queue=result.method.queue)

    queue_name = result.method.queue
    print(f"Queue Name: {queue_name}")
    channel.basic_consume(queue=queue_name, on_message_callback=_consume_youtuber_requests, auto_ack=True)
    print(" [*] Waiting for youtuber requests. To exit press CTRL+C")
    channel.start_consuming()



main()
