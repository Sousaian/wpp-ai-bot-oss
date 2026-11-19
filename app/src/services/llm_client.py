import asyncio
from dataclasses import dataclass
from decimal import Decimal
from functools import lru_cache
from typing import Literal

from loguru import logger
from openai import (
    APIConnectionError,
    APIStatusError,
    APITimeoutError,
    AsyncOpenAI,
    RateLimitError,
)

from src.config import settings


LLMCallType = Literal["resposta", "resumo"]


@dataclass(slots=True)
class LLMResult:
    texto: str
    # Requested, allowlisted model id: billing and exact attempt-link key.
    modelo_llm: str
    tokens_input: int
    tokens_output: int
    tokens_reasoning: int | None
    tokens_cache_read: int | None
    tokens_cache_write: int | None
    custo_creditos: Decimal
    provider_generation_id: str | None
    # Model id exactly as the provider reported it (None when absent).
    modelo_llm_reportado: str | None = None


@lru_cache(maxsize=1)
def _client() -> AsyncOpenAI:
    return AsyncOpenAI(
        api_key=settings.openrouter_api_key,
        base_url=settings.openrouter_base_url,
        timeout=30.0,
        max_retries=0,
    )


def _normalizar_custo(raw_cost: object) -> Decimal:
    if raw_cost is None:
        return Decimal("0")
    try:
        return Decimal(str(raw_cost))
    except Exception:
        return Decimal("0")


def _extrair_custo_creditos(completion: object, usage: object) -> Decimal:
    usage_extra = dict(getattr(usage, "model_extra", {}) or {}) if usage else {}
    completion_extra = dict(getattr(completion, "model_extra", {}) or {})

    for candidate in (
        usage_extra.get("cost"),
        usage_extra.get("total_cost"),
        completion_extra.get("cost"),
        completion_extra.get("total_cost"),
        completion_extra.get("usage", {}).get("cost") if isinstance(completion_extra.get("usage"), dict) else None,
    ):
        value = _normalizar_custo(candidate)
        if value > 0:
            return value

    return Decimal("0")


class LLMProviderError(RuntimeError):
    """A single provider request ended without a usable completion.

    ``outcome_known`` is true only when the provider answered with an HTTP
    error status, so no billable generation is assumed. Every other failure
    (timeout, transport, cancellation, unparseable response) may have been
    charged and must be quarantined, never silently retried.
    """

    def __init__(self, safe_error_code: str, *, outcome_known: bool) -> None:
        super().__init__(safe_error_code)
        self.safe_error_code = safe_error_code
        self.outcome_known = outcome_known


def _resultado(completion: object, modelo_llm: str, *, permitir_vazio: bool = False) -> LLMResult:
    choices = getattr(completion, "choices", None)
    conteudo = choices[0].message.content if choices else None
    if not conteudo and not permitir_vazio:
        raise ValueError("Provider LLM retornou resposta vazia.")

    usage = completion.usage
    prompt_tokens = getattr(usage, "prompt_tokens", 0) or 0
    completion_tokens = getattr(usage, "completion_tokens", 0) or 0

    completion_details = getattr(usage, "completion_tokens_details", None)
    prompt_details = getattr(usage, "prompt_tokens_details", None)

    tokens_reasoning = None
    if completion_details is not None:
        tokens_reasoning = getattr(completion_details, "reasoning_tokens", None)

    tokens_cache_read = None
    tokens_cache_write = None
    if prompt_details is not None:
        tokens_cache_read = getattr(prompt_details, "cached_tokens", None)
        tokens_cache_write = getattr(prompt_details, "cache_creation_tokens", None)

    custo_creditos = _extrair_custo_creditos(completion, usage)

    provider_generation_id = completion.id if completion.id else None

    return LLMResult(
        texto=(conteudo or "").strip(),
        modelo_llm=modelo_llm,
        modelo_llm_reportado=getattr(completion, "model", None) or None,
        tokens_input=int(prompt_tokens),
        tokens_output=int(completion_tokens),
        tokens_reasoning=int(tokens_reasoning) if tokens_reasoning is not None else None,
        tokens_cache_read=int(tokens_cache_read) if tokens_cache_read is not None else None,
        tokens_cache_write=int(tokens_cache_write) if tokens_cache_write is not None else None,
        custo_creditos=custo_creditos,
        provider_generation_id=provider_generation_id,
    )


async def completar(
    *,
    modelo_llm: str,
    messages: list[dict[str, str]],
    tipo_chamada: LLMCallType,
) -> LLMResult:
    last_error: Exception | None = None

    for tentativa in range(1, 3):
        try:
            completion = await _client().chat.completions.create(
                model=modelo_llm,
                messages=messages,
            )

            if not completion.choices or not completion.choices[0].message.content:
                raise ValueError("Provider LLM retornou resposta vazia.")

            return _resultado(completion, modelo_llm)
        except (APITimeoutError, APIConnectionError, RateLimitError) as exc:
            last_error = exc
            logger.warning(
                "Falha temporaria no provider LLM | tentativa={} | tipo_chamada={} | modelo={} | erro={}",
                tentativa,
                tipo_chamada,
                modelo_llm,
                exc.__class__.__name__,
            )
            if tentativa == 2:
                break
        except Exception as exc:
            logger.error(
                "Falha no provider LLM | tipo_chamada={} | modelo={} | erro_tipo={}",
                tipo_chamada,
                modelo_llm,
                type(exc).__name__,
            )
            raise

    raise RuntimeError(
        f"Provider LLM indisponivel apos retries | tipo_chamada={tipo_chamada} | modelo={modelo_llm}"
    ) from last_error


async def completar_tentativa_unica(
    *,
    modelo_llm: str,
    messages: list[dict[str, str]],
    tipo_chamada: LLMCallType,
) -> LLMResult:
    """Issue exactly one provider request for a fenced generation attempt.

    No client retry: an ambiguous failure could already be billable. Empty
    content is returned (``texto == ""``) so its usage is still accounted.
    """
    try:
        completion = await _client().chat.completions.create(
            model=modelo_llm,
            messages=messages,
        )
    except asyncio.CancelledError:
        raise
    except APIStatusError as exc:
        logger.warning(
            "Provider LLM recusou tentativa | tipo_chamada={} | modelo={} | status={}",
            tipo_chamada,
            modelo_llm,
            exc.status_code,
        )
        raise LLMProviderError(
            f"provider_http_{exc.status_code}", outcome_known=True
        ) from exc
    except Exception as exc:
        logger.error(
            "Resultado do provider LLM desconhecido | tipo_chamada={} | modelo={} | erro_tipo={}",
            tipo_chamada,
            modelo_llm,
            type(exc).__name__,
        )
        raise LLMProviderError("provider_outcome_unknown", outcome_known=False) from exc
    try:
        # A billable answer must carry choices and a usage object. Without
        # them the cost is unknown: never record a fictitious zero-cost
        # success. Present usage with empty content stays an accounted call.
        if not getattr(completion, "choices", None) or getattr(completion, "usage", None) is None:
            raise ValueError("provider completion has no choices or usage")
        return _resultado(completion, modelo_llm, permitir_vazio=True)
    except Exception as exc:
        logger.error(
            "Resposta do provider LLM ilegivel | tipo_chamada={} | modelo={} | erro_tipo={}",
            tipo_chamada,
            modelo_llm,
            type(exc).__name__,
        )
        raise LLMProviderError(
            "provider_response_unparseable", outcome_known=False
        ) from exc
