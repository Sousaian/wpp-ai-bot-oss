from collections.abc import Sequence

from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.models import Agente, Mensagem
from src.database.repositories import consumo_repo, mensagem_repo, sessao_repo
from src.services import llm_client


TRANSFER_TAG = "[TRANSFERIR]"
# Single source for the handoff-summary instruction (legacy and scheduled).
PROMPT_RESUMO_HANDOFF = (
    "Você gera resumos de conversas de vendas para handoff humano. "
    "Responda de forma objetiva, em até 5 linhas, incluindo: "
    "o que o lead busca, principais dúvidas/objeções e estado atual."
)


class InvalidLLMContentError(RuntimeError):
    """A billable generation completed but produced no deliverable content."""


def resolver_modelo_llm(agente: Agente) -> str:
    modelo_llm = getattr(agente, "modelo_llm", None)
    if isinstance(modelo_llm, str) and modelo_llm.strip():
        return modelo_llm.strip()
    return agente.modelo_openai


_resolver_modelo_llm = resolver_modelo_llm


def normalizar_resposta(resposta: str) -> tuple[str, bool]:
    """Strip the transfer tag before any persistence or delivery."""
    resposta_raw = resposta.strip()
    needs_transfer = TRANSFER_TAG in resposta_raw
    return resposta_raw.replace(TRANSFER_TAG, "").strip(), needs_transfer


def montar_contexto_turno(
    system_prompt: str,
    historico_anterior: Sequence[tuple[str, str]],
    lote_atual: Sequence[tuple[int, str]],
) -> list[dict[str, str]]:
    """Context for a sealed scheduled turn.

    ``historico_anterior`` is bounded accepted history as (role, content);
    ``lote_atual`` is the entire sealed batch as (message id, content), in
    input-sequence order. The batch is appended once and is never truncated
    by ``max_historico``; equal consecutive texts stay distinct inputs.
    """
    if not lote_atual:
        raise ValueError("sealed batch must not be empty")
    if len({mensagem_id for mensagem_id, _ in lote_atual}) != len(lote_atual):
        raise ValueError("sealed batch repeats a message identity")
    messages: list[dict[str, str]] = [{"role": "system", "content": system_prompt}]
    messages += [{"role": role, "content": conteudo} for role, conteudo in historico_anterior]
    messages += [{"role": "user", "content": conteudo} for _, conteudo in lote_atual]
    return messages


def _montar_messages(agente: Agente, historico: list[Mensagem], mensagem_atual: str) -> list[dict[str, str]]:
    messages: list[dict[str, str]] = [{"role": "system", "content": agente.system_prompt}]
    messages += [{"role": m.role, "content": m.conteudo} for m in historico]

    # Se o repositorio ja trouxe a mensagem atual no historico, evita duplicacao.
    if not historico or historico[-1].role != "user" or historico[-1].conteudo != mensagem_atual:
        messages.append({"role": "user", "content": mensagem_atual})

    return messages


async def _registrar_consumo_seguro(
    *,
    db: AsyncSession,
    agente: Agente,
    sessao_id: int,
    llm_result: llm_client.LLMResult,
    tipo_chamada: str,
) -> None:
    try:
        await consumo_repo.registrar_ou_reconciliar(
            db,
            agente_id=agente.id,
            sessao_id=sessao_id,
            modelo_llm=llm_result.modelo_llm,
            tipo_chamada=tipo_chamada,
            tokens_input=llm_result.tokens_input,
            tokens_output=llm_result.tokens_output,
            tokens_reasoning=llm_result.tokens_reasoning,
            tokens_cache_read=llm_result.tokens_cache_read,
            tokens_cache_write=llm_result.tokens_cache_write,
            custo_creditos=llm_result.custo_creditos,
            provider_generation_id=llm_result.provider_generation_id,
            modelo_llm_reportado=llm_result.modelo_llm_reportado,
        )
    except consumo_repo.UsagePersistenceInvariantError:
        raise
    except Exception as exc:
        raise consumo_repo.UsagePersistenceInvariantError(
            "successful generation has no verified durable usage outcome"
        ) from exc


async def processar(
    agente: Agente,
    sessao_id: int,
    mensagem: str,
    db: AsyncSession,
) -> tuple[str, bool]:
    """
    Processa uma mensagem de usuario com o modelo configurado no agente.

    Retorna:
        (texto_resposta_sem_flag, needs_transfer)
    """
    await mensagem_repo.salvar(db, sessao_id, "user", mensagem)
    historico = await mensagem_repo.get_historico(
        db,
        sessao_id,
        agente.max_historico,
        current_read=True,
    )
    messages = _montar_messages(agente, historico, mensagem)
    modelo_llm = _resolver_modelo_llm(agente)

    llm_result = await llm_client.completar(
        modelo_llm=modelo_llm,
        messages=messages,
        tipo_chamada="resposta",
    )

    texto_resposta, needs_transfer = normalizar_resposta(llm_result.texto)

    if texto_resposta:
        await mensagem_repo.salvar(db, sessao_id, "assistant", texto_resposta)

    # Usage is a commercial fact even when normalized output is unusable. Keep
    # it in the same UoW before selecting a controlled fallback or handoff.
    await _registrar_consumo_seguro(
        db=db,
        agente=agente,
        sessao_id=sessao_id,
        llm_result=llm_result,
        tipo_chamada="resposta",
    )

    if not texto_resposta and not needs_transfer:
        raise InvalidLLMContentError("LLM response has no deliverable content")

    await sessao_repo.incrementar_mensagens(db, sessao_id)

    logger.info(
        "Resposta gerada para sessao={} usando modelo={} transfer={}",
        sessao_id,
        llm_result.modelo_llm,
        needs_transfer,
    )

    return texto_resposta, needs_transfer


async def gerar_resumo(sessao_id: int, agente: Agente, db: AsyncSession) -> str:
    """
    Gera um resumo curto da conversa para handoff humano.
    """
    historico = await mensagem_repo.get_historico(
        db,
        sessao_id,
        limite=30,
        current_read=True,
    )
    if not historico:
        return "Sem histórico de conversa disponível para resumo."

    messages = [
        {
            "role": "system",
            "content": PROMPT_RESUMO_HANDOFF,
        }
    ] + [{"role": mensagem.role, "content": mensagem.conteudo} for mensagem in historico]

    llm_result = await llm_client.completar(
        modelo_llm=_resolver_modelo_llm(agente),
        messages=messages,
        tipo_chamada="resumo",
    )

    await _registrar_consumo_seguro(
        db=db,
        agente=agente,
        sessao_id=sessao_id,
        llm_result=llm_result,
        tipo_chamada="resumo",
    )

    return llm_result.texto.strip()
