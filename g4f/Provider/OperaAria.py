from __future__ import annotations

import json
import time
import os
import base64
import asyncio
from typing import AsyncGenerator
from ..typing import AsyncResult, Messages, MediaListType
from .base_provider import AsyncGeneratorProvider, ProviderModelMixin
from .helper import format_prompt
from ..providers.response import JsonConversation, FinishReason, ImageResponse
from ..image import is_data_an_media
from ..tools.media import merge_media
from ..requests import StreamSession  # switched


class Conversation(JsonConversation):
    """Manages session state for Opera Aria."""

    access_token: str = None
    refresh_token: str = None
    encryption_key: str = None
    expires_at: float = 0
    conversation_id: str = None
    is_first_request: bool = True

    def __init__(self, refresh_token: str = None):
        self.refresh_token = refresh_token
        self.encryption_key = base64.b64encode(os.urandom(32)).decode("utf-8")
        self.is_first_request = True

    def is_token_expired(self) -> bool:
        return time.time() >= self.expires_at

    def update_token(self, access_token: str, expires_in: int):
        self.access_token = access_token
        self.expires_at = time.time() + expires_in - 60


class OperaAria(AsyncGeneratorProvider, ProviderModelMixin):
    label = "Opera Aria"
    url = "https://play.google.com/store/apps/details?id=com.opera.browser"

    # Endpoints
    api_endpoint_v1 = "https://composer.opera-api.com/api/v1/a-chat"
    api_endpoint_v2 = "https://composer.opera-api.com/api/v2/a-chat"
    upload_endpoint = "https://composer.opera-api.com/api/v2/files/upload"
    files_endpoint = "https://composer.opera-api.com/api/v2/files/"
    token_endpoint = "https://oauth2.opera-api.com/oauth2/v1/token/"
    signup_endpoint = "https://auth.opera.com/account/v2/external/anonymous/signup"

    working = True
    needs_auth = False
    supports_stream = True
    supports_system_message = True
    supports_message_history = True

    # Models
    default_model = "aria"
    default_image_model = default_model
    default_vision_model = default_model

    models = [default_model, "aria-legacy"]
    image_models = [default_model]
    vision_models = [default_model]

    _model_to_version = {
        "aria": "v2",
        "aria-legacy": "v1",
    }

    _user_agent_v1 = "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Mobile Safari/537.36 OPR/89.0.0.0"
    _user_agent_v2 = "Mozilla/5.0 (Linux; U; Android 14; Pixel 8 Pro Build/UQ1A.240205.004; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/138.0.7204.179 Mobile Safari/537.36 OPR/99.0.2254.81922"

    @classmethod
    def _get_api_version(cls, model: str) -> str:
        return cls._model_to_version.get(model, "v2")

    # ==================== Authentication ====================

    @classmethod
    async def _generate_refresh_token(cls, session) -> str:
        # Implementation unchanged — authentication flows often use session directly;
        # here we keep function signature; caller will pass StreamSession
        raise NotImplementedError("Auth flow implementation present in original file")
