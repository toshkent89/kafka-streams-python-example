import faust

app = faust.App(
    'color_filter',
    broker='kafka://kafka.broker:9092',
    value_serializer='json',
)

class Message(faust.Record):
    key: str
    value: str

# Определите топики
input_topic = app.topic('your-topic-input', value_type=Message)
output_topic = app.topic('your-topic-output')

@app.agent(input_topic)
async def process(stream):
    async for message in stream:
        msg_key = message.key
        msg_value = message.value
        try:
          color, N = msg_value.split('-')
          N = int(N)
        except ValueError:
          print(f"Error processing message: invalid format or value {msg_str}")
          continue
        N += 1
        if color in ['green', 'yellow'] and N % 3 == 0:
            new_value = f"{color}-{N}"
            await output_topic.send(key=msg_key, value=new_value)
            print(f"Processed and sent: Key: {msg_key} Value: {new_value}")

if __name__ == '__main__':
    app.main()