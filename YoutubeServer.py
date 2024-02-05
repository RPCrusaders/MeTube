import pika
import sys

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()


def main():
    channel.exchange_declare(exchange="content", exchange_type="direct")
    youtuber = "natsu"
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue
    print(f"Queue Name: {queue_name}, Youtuber: {youtuber}")
    channel.queue_bind(
        exchange="content",
        queue=queue_name,
        routing_key=youtuber
    )

    def callback(ch, method, properties, body):
        print(f"[x] Received {body}")
    
    print("Waiting for messages")
    while True:
        channel.basic_consume(
            queue=queue_name,
            on_message_callback=callback,
            auto_ack=True
        )



main()
