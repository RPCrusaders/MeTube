import pika
import sys
import json


connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

youtubers = set()
users = set()
users_subscribed = {}

def _consume_youtuber_requests(ch, method, properties, body):
    body = json.loads(body)
    youtubers.add(body['youtuber'])
    print(f"{body['youtuber']} uploaded {body['video_name']}")
    # print(body)

def _consume_user_requests(ch, method, properties, body):
    body = json.loads(body)
    if body['user'] not in users:
        users.add(body['user'])
        channel.queue_declare(queue=body['user'], durable=True)
    if body['user'] not in users_subscribed:
        users_subscribed[body['user']] = set()
    if body['youtuber'] in youtubers:
        if body['status'] == "subscribed":
            if body['youtuber'] in users_subscribed.get(body['user'], set()):
                print(f"{body['user']} tried to subscribe to {body['youtuber']} but was already subscribed")
                return
            print(f"{body['user']} subscribed {body['youtuber']}")
            channel.queue_bind(exchange="content", queue=body['user'], routing_key=body['youtuber'])
            users_subscribed.get(body['user'], set()).add(body['youtuber'])
        else:
            if body['youtuber'] not in users_subscribed.get(body['user'], set()):
                print(f"{body['user']} tried to unsubscribe from {body['youtuber']} but was not subscribed")
                return
            print(f"{body['user']} unsubscribed {body['youtuber']}")
            channel.queue_unbind(exchange="content", queue=body['user'], routing_key=body['youtuber'])
            users_subscribed.get(body['user'], set()).remove(body['youtuber'])
    else:
        print(f"{body['user']} tried to subscribe to {body['youtuber']} but {body['youtuber']} does not exist")
    # print(body)

def consume_requests(ch, method, properties, body):
    print(f" [x] Received {body}")
    context = json.loads(body)
    if context['status'] == "uploaded":
        _consume_youtuber_requests(ch, method, properties, body)
    else:
        _consume_user_requests(ch, method, properties, body)

def main():
    channel.exchange_declare(exchange="content", exchange_type="direct", durable=True)
    channel.exchange_declare(exchange="server_info", exchange_type="fanout", durable=True)
    result = channel.queue_declare(queue="", exclusive=True)

    # binding the queue to the exchange
    channel.queue_bind(exchange="server_info", queue=result.method.queue)

    queue_name = result.method.queue
    print(f"Queue Name: {queue_name}")
    channel.basic_consume(queue=queue_name, on_message_callback=consume_requests, auto_ack=True)
    print(" [*] Waiting for youtuber requests. To exit press CTRL+C")
    channel.start_consuming()



main()
