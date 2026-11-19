from collections.abc import Mapping
import secrets

import httpx
from loguru import logger

from src.config import settings


EVOLUTION_WEBHOOK_EVENTS = (
    "MESSAGES_UPSERT",
    "MESSAGES_UPDATE",
    "CONNECTION_UPDATE",
)


class EvolutionWebhookContractError(RuntimeError):
    """Installed Evolution webhook configuration is missing or incompatible."""


class EvolutionClient:
    """Client for Evolution API communication"""
    
    def __init__(self):
        self.base_url = settings.evolution_api_url
        self.api_key = settings.evolution_api_key
        self.headers = {
            "apikey": self.api_key,
            "Content-Type": "application/json"
        }
    
    async def send_text_message(self, phone: str, message: str, instance_name: str) -> dict:
        """
        Send text message via Evolution API
        
        Args:
            phone: Phone number (format: 5562999999999)
            message: Text message to send
            instance_name: Nome da instância na Evolution API
            
        Returns:
            dict: Response from Evolution API
        """
        try:
            # Ensure phone has @s.whatsapp.net suffix
            if not phone.endswith("@s.whatsapp.net"):
                remote_jid = f"{phone}@s.whatsapp.net"
            else:
                remote_jid = phone
            
            url = f"{self.base_url}/message/sendText/{instance_name}"
            
            payload = {
                "number": remote_jid,
                "text": message
            }
            
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    url,
                    headers=self.headers,
                    json=payload,
                    timeout=30.0
                )
                
                response.raise_for_status()
                result = response.json()
                
                logger.info(
                    "✉️ Message sent | instance={} status={}",
                    instance_name,
                    response.status_code,
                )
                return result
        
        except httpx.HTTPStatusError as e:
            logger.error(
                "❌ HTTP error sending message: status={} instance={}",
                e.response.status_code,
                instance_name,
            )
            raise
        except Exception as e:
            logger.error(
                "❌ Error sending message: error_type={} instance={}",
                type(e).__name__,
                instance_name,
            )
            raise
    
    async def send_file(
        self,
        phone: str,
        file_url: str,
        instance_name: str,
        caption: str | None = None,
    ) -> dict:
        """
        Send file/media via Evolution API
        
        Args:
            phone: Phone number
            file_url: URL of the file to send
            instance_name: Nome da instância na Evolution API
            caption: Optional caption for the file
            
        Returns:
            dict: Response from Evolution API
        """
        try:
            if not phone.endswith("@s.whatsapp.net"):
                remote_jid = f"{phone}@s.whatsapp.net"
            else:
                remote_jid = phone
            
            url = f"{self.base_url}/message/sendMedia/{instance_name}"
            
            payload = {
                "number": remote_jid,
                "mediatype": "document",
                "media": file_url
            }
            
            if caption:
                payload["caption"] = caption
            
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    url,
                    headers=self.headers,
                    json=payload,
                    timeout=30.0
                )
                
                response.raise_for_status()
                result = response.json()
                
                logger.info("📎 File sent | instance={}", instance_name)
                return result
        
        except Exception as e:
            logger.error(
                "❌ Error sending file | instance={} error_type={}",
                instance_name,
                type(e).__name__,
            )
            raise
    
    async def get_instance_status(self, instance_name: str) -> dict:
        """Get instance connection status"""
        try:
            url = f"{self.base_url}/instance/connectionState/{instance_name}"
            
            async with httpx.AsyncClient() as client:
                response = await client.get(url, headers=self.headers)
                response.raise_for_status()
                return response.json()
        
        except Exception as e:
            logger.error(
                "❌ Error getting instance status | instance={} error_type={}",
                instance_name,
                type(e).__name__,
            )
            raise
    
    async def set_webhook(
        self,
        webhook_url: str,
        instance_name: str,
        *,
        bearer_token: str | None = None,
    ) -> dict:
        """
        Configure webhook for the instance
        
        Args:
            webhook_url: Your webhook URL (e.g., ngrok URL)
            instance_name: Nome da instância na Evolution API
            
        Returns:
            dict: Response from Evolution API
        """
        token = bearer_token or settings.evolution_webhook_bearer_token
        if not token:
            raise EvolutionWebhookContractError(
                "Evolution webhook bearer is not configured"
            )

        try:
            url = f"{self.base_url}/webhook/set/{instance_name}"
            
            payload = {
                "webhook": {
                    "enabled": True,
                    "url": webhook_url,
                    "headers": {
                        "Authorization": f"Bearer {token}"
                    },
                    "byEvents": False,
                    "base64": False,
                    "events": list(EVOLUTION_WEBHOOK_EVENTS),
                }
            }
            
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    url,
                    headers=self.headers,
                    json=payload,
                    timeout=30.0
                )
                
                response.raise_for_status()
                result = response.json()
                
                logger.info("🔗 Webhook configured | instance={}", instance_name)
                return result
        
        except Exception as e:
            logger.error(
                "❌ Error setting webhook | instance={} error_type={}",
                instance_name,
                type(e).__name__,
            )
            raise

    async def get_webhook(self, instance_name: str) -> Mapping[str, object]:
        """Fetch one installed instance webhook without logging its body."""
        url = f"{self.base_url}/webhook/find/{instance_name}"
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(url, headers=self.headers)
                response.raise_for_status()
                data = response.json()
        except Exception as exc:
            logger.error(
                "Error reading Evolution webhook | instance={} error_type={}",
                instance_name,
                type(exc).__name__,
            )
            raise EvolutionWebhookContractError(
                "Evolution webhook lookup failed"
            ) from None

        if not isinstance(data, Mapping):
            raise EvolutionWebhookContractError(
                "Evolution webhook lookup returned an incompatible response"
            )
        return data

    async def verify_webhook(
        self,
        webhook_url: str,
        instance_name: str,
        *,
        bearer_token: str | None = None,
    ) -> None:
        """Verify the exact URL/header contract installed for an instance."""
        token = bearer_token or settings.evolution_webhook_bearer_token
        if not token:
            raise EvolutionWebhookContractError(
                "Evolution webhook bearer is not configured"
            )

        data = await self.get_webhook(instance_name)
        nested = data.get("webhook")
        webhook = nested if isinstance(nested, Mapping) else data
        configured_url = webhook.get("url") or webhook.get("webhookUrl")
        enabled = webhook.get("enabled")
        headers = webhook.get("headers") or webhook.get("webhookHeaders")
        events = webhook.get("events")

        if configured_url != webhook_url or enabled is not True:
            raise EvolutionWebhookContractError(
                "Evolution webhook URL or enabled state does not match"
            )
        if not isinstance(headers, Mapping):
            raise EvolutionWebhookContractError(
                "Evolution webhook headers are missing"
            )

        authorization = headers.get("Authorization") or headers.get("authorization")
        if not isinstance(authorization, str) or not secrets.compare_digest(
            authorization.encode(), f"Bearer {token}".encode()
        ):
            raise EvolutionWebhookContractError(
                "Evolution webhook bearer does not match"
            )

        if not isinstance(events, list) or not set(EVOLUTION_WEBHOOK_EVENTS).issubset(
            {str(event).upper() for event in events}
        ):
            raise EvolutionWebhookContractError(
                "Evolution webhook events do not match"
            )


# Singleton instance
evolution_client = EvolutionClient()
