import pika
import sys
import json




def main():
    if len(sys.argv) < 3:
        print("Invalid Arguments")
        return 
    

    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.exchange_declare(exchange="content", exchange_type="direct", durable=True)
    channel.exchange_declare(exchange="server_info", exchange_type="fanout", durable=True)

    youtuber = "".join(sys.argv[1])
    video_name = " ".join(sys.argv[2:])
    channel.basic_publish(
        exchange="content",
        routing_key=youtuber,
        body= json.dumps({"youtuber": youtuber, "video_name": video_name})
    )

    channel.basic_publish(
        exchange="server_info",
        routing_key="info",
        body=json.dumps({"youtuber": youtuber, "video_name": video_name, "status": "uploaded"}),
        properties=pika.BasicProperties(delivery_mode=2), # make message persistent
    )
    print(f"[x] Sent {youtuber}:{video_name}")

    print("SUCCESS")
    connection.close()

# if __name__ == "main":
main()
