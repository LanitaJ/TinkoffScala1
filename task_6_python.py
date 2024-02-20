import asyncio
from enum import Enum
from typing import List
from datetime import datetime, timedelta
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
import random

@dataclass
class Payload:
    data: str  # Простое представление данных

@dataclass
class Address:
    email: str  # Пример адреса получателя

@dataclass
class Event:
    recipients: List[Address]
    payload: Payload

class Result(Enum):
    Accepted = 1
    Rejected = 2

async def read_data() -> Event:
    # Имитация чтения данных: возвращаем событие с одним получателем и простым текстом в качестве данных
    return Event(
        recipients=[Address(email="recipient@example.com")],
        payload=Payload(data="Hello, this is a test payload!")
    )

async def send_data(dest: Address, payload: Payload) -> Result:
    # Имитация отправки данных: случайный успех или неудача
    if random.choice([True, False]):
        print(f"Sending data to {dest.email} succeeded: {payload.data}")
        return Result.Accepted
    else:
        print(f"Sending data to {dest.email} failed: {payload.data}")
        return Result.Rejected

async def perform_operation():
    while True:  # Бесконечный цикл для непрерывной обработки
        event = await read_data()  # Чтение данных
        for recipient in event.recipients:
            result = await send_data(recipient, event.payload)  # Отправка данных
            if result == Result.Rejected:
                print(f"Retrying to send data to {recipient.email}")
                await asyncio.sleep(1)  # Краткая пауза перед повторной отправкой

# Инициализация и запуск асинхронной операции
async def main():
    # Запуск в контексте ThreadPoolExecutor, если потребуется включить блокирующие функции
    executor = ThreadPoolExecutor()
    asyncio.get_running_loop().set_default_executor(executor)
    await perform_operation()

asyncio.run(main())
