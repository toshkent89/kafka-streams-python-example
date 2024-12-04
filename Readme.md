# Пример kafka-streams на библиотеке faust (python)

1. Генерируем сообщения в формате json с помощью kafka-client.py (сообщения вида key: AA, value: green-100 ) - `kafka-client.py producer <your_topic_input>`
2. Запускаем kafka-stream - `python kafka-streams.py worker` (не забываем поменять имя топика в коде)
3. Смотрим в результирующий топик на преобразованные данные - `kafka-client.py consumer <your_topic_output>`