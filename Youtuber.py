import pika
import sys




def main():
    if len(sys.argv) < 3:
        print("Invalid Arguments")
        return 
    

    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.exchange_declare(exchange="content", exchange_type="direct")


    youtuber = "natsu"
    video_name = " ".join(sys.argv[2:])
    channel.basic_publish(
        exchange="content",
        routing_key=youtuber,
        body=video_name
    )
    print(f"[x] Sent {youtuber}:{video_name}")

    print("SUCCESS")


# if __name__ == "main":
main()
