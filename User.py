import pika
import sys
import json


connection = pika.BlockingConnection(pika.ConnectionParameters(host='34.16.198.68', port='5672',
                                                            credentials= pika.credentials.PlainCredentials('a','a')
                                                            ))
channel = connection.channel()

def callback(ch, method, properties, body):
    # print(f" [x] Received {body}")
    body = json.loads(body)
    print(f"{body['youtuber']} uploaded {body['video_name']}")
    # print(body)

def main():
    if len(sys.argv) != 2 and len(sys.argv) != 4:
        print("Invalid Arguments")
        return
    print(sys.argv)
    # connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    # channel = connection.channel()
    channel.exchange_declare(exchange="content", exchange_type="direct", durable=True)
    channel.exchange_declare(exchange="server_info", exchange_type="fanout", durable=True)
    user = "".join(sys.argv[1])
    channel.queue_declare(queue=user, durable=True)
    if len(sys.argv) == 4:
        flag = "".join(sys.argv[2])
        youtuber = "".join(sys.argv[3])
        if flag == "-S":
            status = "subscribed"
        elif flag == "-U":
            status = "unsubscribed"
        else:
            print("Invalid Arguments")
            return
        print(f"User: {user}, Flag: {flag}, Youtuber: {youtuber}")
        channel.basic_publish(
            exchange="server_info",
            routing_key="info",
            body=json.dumps({"user": user, "youtuber": youtuber, "status": status}),
            properties=pika.BasicProperties(delivery_mode=2), # make message persistent
            # mandatory=True
        )
        print(f"[x] Sent {user}:{youtuber}")

    channel.basic_consume(queue=user, on_message_callback=callback, auto_ack=True)
    channel.start_consuming()



main()
