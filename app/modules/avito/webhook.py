# app/modules/avito/webhook.py
import hmac
import hashlib
import json
import asyncio
from typing import Optional
from fastapi import Request, Header, HTTPException, Response
import redis.asyncio as redis

from app.core.config import env  # Используем новый конфиг
from app.core.logger import log

class AvitoWebhookHandler:
    """
    Принимает удар от Авито. 
    Задача: Проверить подпись -> Положить в Redis -> Ответить 200 OK.
    Никаких баз данных и сложной логики!
    """
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client

    async def verify_signature(self, payload: bytes, signature: str) -> bool:
        """Проверяет X-Signature."""
        if not env.avito_webhook_secret:
            # Если секрет не задан в .env - считаем, что проверка отключена (для тестов)
            return True
            
        if not signature:
            log.warning("⚠️ Webhook received without X-Signature header.")
            return False
            
        secret = env.avito_webhook_secret.encode('utf-8')
        expected_signature = hmac.new(secret, msg=payload, digestmod=hashlib.sha256).hexdigest()
        
        return hmac.compare_digest(expected_signature, signature)

    async def handle_request(self, request: Request, x_signature: Optional[str] = Header(None)):
        # 1. Читаем байты (нужны для проверки подписи)
        payload_bytes = await request.body()
        
        try:
            payload_bytes = await request.body()
            payload = json.loads(payload_bytes)
        except json.JSONDecodeError:
            return Response(content="Bad JSON", status_code=400)

        # 3. Фильтрация мусора (не сообщений)
        # Авито шлет много событий (message, read, delivery). Нам пока нужны только message.
        event_type = payload.get("payload", {}).get("type")
        if event_type != "message":
            # Отвечаем ОК, чтобы Авито не слало повторно, но не обрабатываем
            return Response(content="Ignored event type", status_code=200)

        # 4. Кладем в Redis Stream "как есть" (Raw Event)
        # Добавляем ID события для идемпотентности в воркере
        event_id = payload.get("payload", {}).get("value", {}).get("id")
        
        stream_data = {
            "event_id": str(event_id),
            "raw_body": payload_bytes.decode('utf-8') # Сохраняем как строку
        }

        # Используем maxlen, чтобы стрим не забил память, если воркеры упадут
        await self.redis.xadd("stream:avito:raw_webhooks", stream_data, maxlen=10000)
        
        log.info(f"📥 Received webhook msg_id={event_id}. Queued to stream.")
        
        return Response(content="ok", status_code=200)