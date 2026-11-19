-- Deliberately corrupt predecessor fixture: one ledger backs two attempts.
-- Only for a disposable database installed from a pre-round1 schema.
SET @agent=(SELECT MIN(id) FROM agentes);
SET @session=(SELECT MIN(id) FROM sessoes);
SET @turn=(SELECT MIN(id) FROM conversation_turns);
SET @revision=(SELECT MIN(id) FROM turn_revisions);
INSERT INTO consumo_llm (agente_id,sessao_id,modelo_llm,tipo_chamada,tokens_input,tokens_output,custo_creditos)
VALUES (@agent,@session,'openai/gpt-4o-mini','resposta',11,5,0.00002000);
SET @consumption=LAST_INSERT_ID();
INSERT INTO generation_attempts
(attempt_key,agente_id,sessao_id,turn_id,revision_id,scheduler_epoch,lane_owner_token,
 attempt_type,attempt_ordinal,outcome,accounting_state,modelo_llm,tokens_input,tokens_output,
 custo_creditos,consumo_llm_id,outcome_recorded_at)
SELECT UUID(),@agent,@session,@turn,@revision,'22222222-2222-4222-8222-222222222222',UUID(),
 'response',ordinal,'succeeded','recorded','openai/gpt-4o-mini',11,5,0.00002000,@consumption,UTC_TIMESTAMP(6)
FROM (SELECT 1 AS ordinal UNION ALL SELECT 2) AS attempts;
