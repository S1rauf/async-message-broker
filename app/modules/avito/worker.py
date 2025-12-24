# /app/modules/avito/worker.py

import asyncio
import logging
from typing import Optional
import json
from datetime import datetime, timezone

import redis.asyncio as redis
from sqlalchemy import select

from app.db_models import MessageLog, AvitoAccount
from app.core.database import get_session
from ..telegram.bot import bot
from ..telegram.view_provider import (
    VIEW_KEY_TPL, rehydrate_view_model, VIEW_TTL_SECONDS, get_all_subscribers
)
from ..telegram.view_renderer import ViewRenderer
from ..telegram.history_manager import hot_update_chat_history
from .client import AvitoAPIClient
from .messaging import AvitoMessaging
from .actions import AvitoChatActions


logger = logging.getLogger(__name__)


async def process_outgoing_messages(redis_client: redis.Redis):
    """
    Слушает стрим avito:outgoing:messages, отправляет сообщения в Avito.
    Логирует исходящее сообщение в БД.
    Обновляет ChatViewModel и запускает перерисовку ТОЛЬКО для ручных действий.
    """
    stream_name = "avito:outgoing:messages"
    group_name = "avito_workers"
    consumer_name = "outgoing_consumer_1"
    renderer = ViewRenderer(bot, redis_client)

    try:
        await redis_client.xgroup_create(stream_name, group_name, id="0", mkstream=True)
        logger.info(f"Consumer group '{group_name}' created for stream '{stream_name}'.")
    except redis.ResponseError as e:
        if "BUSYGROUP" in str(e):
            logger.info(f"Consumer group '{group_name}' for stream '{stream_name}' already exists.")
        else:
            raise

    while True:
        try:
            events = await redis_client.xreadgroup(
                group_name, consumer_name, {stream_name: ">"}, count=1, block=5000
            )
            
            if not events:
                continue

            for _, messages in events:
                for message_id, data in messages:
                    logger.info(f"AVITO_WORKER: Processing outgoing Avito message {message_id} with data {data}")
                    
                    account_id = int(data['account_id'])
                    chat_id = data['chat_id']
                    action_type = data.get("action_type", "manual_reply")
                    
                    async with get_session() as session:
                        account = await session.get(AvitoAccount, account_id)
                        
                        if not (account and account.is_active):
                            logger.warning(f"Account {account_id} not found or inactive. Skipping message.")
                            await redis_client.xack(stream_name, group_name, message_id)
                            continue

                        try:
                            api_client = AvitoAPIClient(account, redis_client=redis_client)
                            messaging = AvitoMessaging(api_client)
                            sent_text_for_log = data.get('text', '')
                            
                            # 1. Отправка сообщения в Avito
                            if action_type == "image_reply":
                                image_id = data['image_id']
                                await messaging.send_image_message(session=session, chat_id=chat_id, image_id=image_id, text=sent_text_for_log)
                                if not sent_text_for_log: sent_text_for_log = "[Изображение]"
                            else:
                                await messaging.send_text_message(session=session, chat_id=chat_id, text=sent_text_for_log)
                            logger.info(f"AVITO_WORKER: Successfully sent message to Avito chat {chat_id}")

                            # 2. Логирование в БД
                            is_autoreply = action_type == "auto_reply"
                            trigger_name = None
                            if is_autoreply:
                                trigger_name = data.get("rule_name")
                            elif action_type == "template_reply":
                                trigger_name = data.get("template_name")
                            
                            log_entry_db = MessageLog(
                                account_id=account.id,
                                chat_id=chat_id,
                                direction='out',
                                is_autoreply=is_autoreply,
                                trigger_name=trigger_name
                            )
                            session.add(log_entry_db)
                            await session.commit()
                            logger.info(f"AVITO_WORKER: Logged outgoing message for chat {chat_id} to DB.")

                        except Exception as e:
                            logger.error(f"AVITO_WORKER: Failed to send message for account {account_id}: {e}", exc_info=True)
                    
                    await redis_client.xack(stream_name, group_name, message_id)

        except Exception as e:
            logger.error(f"Critical error in 'process_outgoing_messages' worker: {e}", exc_info=True)
            await asyncio.sleep(5)


async def process_chat_actions(redis_client: redis.Redis):
    """
    Слушает очередь 'avito:chat:actions' и выполняет действия (например, 'прочитано').
    """
    stream_name = "avito:chat:actions"
    group_name = "avito_action_workers"
    consumer_name = "action_consumer_1"
    renderer = ViewRenderer(bot, redis_client)

    try:
        await redis_client.xgroup_create(stream_name, group_name, id="0", mkstream=True)
    except redis.ResponseError as e:
        if "BUSYGROUP" not in str(e): raise

    while True:
        try:
            events = await redis_client.xreadgroup(
                group_name, consumer_name, {stream_name: ">"}, count=1, block=5000
            )
            if not events: continue

            for _, messages in events:
                for message_id, data in messages:
                    logger.info(f"AVITO_ACTIONS_WORKER: Processing action {message_id} with data: {data}")
                    
                    account_id = int(data['account_id'])
                    chat_id = data['chat_id']
                    action_type = data['action']
                    
                    async with get_session() as session:
                        account = await session.get(AvitoAccount, account_id)
                        if not (account and account.is_active):
                            logger.warning(f"Account {account_id} not found/inactive for chat action.")
                            await redis_client.xack(stream_name, group_name, message_id)
                            continue
                        
                        try:
                            # --- ИСПРАВЛЕНИЕ: Передаем redis_client и session ---
                            api_client = AvitoAPIClient(account, redis_client=redis_client)
                            actions = AvitoChatActions(api_client)
                            
                            if action_type == "mark_read":
                                await actions.mark_as_read(session=session, chat_id=chat_id)
                                
                                view_key = VIEW_KEY_TPL.format(account_id=account.id, chat_id=chat_id)
                                model_json = await redis_client.get(view_key)
                                if model_json:
                                    model = json.loads(model_json)
                                    model["is_last_message_read"] = True
                                    await redis_client.set(view_key, json.dumps(model), keepttl=True)
                                    # Вызов renderer.update_all_subscribers() УДАЛЕН
                                    logger.info(f"ACTIONS_WORKER: ViewModel for {view_key} marked as read.")

                                if not model:
                                    logger.warning(f"ACTIONS_WORKER: No view model for {view_key}. Rehydrating.")
                                    model = await rehydrate_view_model(redis_client, account, chat_id)
                                    if not model:
                                        logger.error(f"ACTIONS_WORKER: Failed to rehydrate model for {view_key}.")
                                        await redis_client.xack(stream_name, group_name, message_id)
                                        continue
                                
                                model["is_last_message_read"] = True
                                await redis_client.set(view_key, json.dumps(model), keepttl=True)
                                
                                logger.info(f"ACTIONS_WORKER: Triggering rerender for {view_key} after mark_read.")
                                await renderer.update_all_subscribers(view_key, model)
                            else:
                                logger.warning(f"AVITO_ACTIONS_WORKER: Received unknown action type '{action_type}'")

                        except Exception as e:
                            logger.error(f"AVITO_ACTIONS_WORKER: Failed to perform action {action_type}: {e}", exc_info=True)

                    await redis_client.xack(stream_name, group_name, message_id)

        except Exception as e:
            logger.error(f"Critical error in 'process_chat_actions' worker: {e}", exc_info=True)
            await asyncio.sleep(5)

async def start_avito_outgoing_worker(redis_client: redis.Redis):
    """
    Запускает все асинхronные задачи, связанные с исходящими действиями Avito.
    """
    logger.info("Starting Avito workers (messages and actions)...")
    
    await asyncio.gather(
        process_outgoing_messages(redis_client),
        process_chat_actions(redis_client)
    )