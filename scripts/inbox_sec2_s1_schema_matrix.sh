#!/usr/bin/env bash
# R2-S1: sourced by T082; uses only its disposable server and durable logs.
sec2_s1_schema_matrix() {
    local dir="$workdir/sec2-s1-schema" charset db repeat cut tamper total_up=0 total_int=0 total_tamper=0 total_mut=0
    mkdir -p "$dir"
    git -C "$repo_root" show f57ad6e:db/init.sql > "$dir/base.sql" || return $?
    python - "$repo_root/db/init.sql" "$dir" <<'PY' || return $?
from pathlib import Path
import sys
sql=Path(sys.argv[1]).read_text(); root=Path(sys.argv[2])
suffix=sql[sql.index('-- R2-S1: indexed retention SLA'):]
start=suffix.index('    IF actual_shape IS NULL OR actual_shape <> CONCAT(')
end=suffix.index(' THEN', start)
(root/'mutant.sql').write_text(suffix[:start]+'    IF FALSE'+suffix[end:])
for i,marker in enumerate(('index-created','index-validated')):
    (root/f'int-{i}.sql').write_text(suffix[:suffix.index('-- sec2-s1-schema-boundary: '+marker)]+'\nDELIMITER ;\n')
for name,definition in {
 'order':'(terminal_at, session_slot)', 'missing-column':'(session_slot)',
 'extra-column':'(session_slot, terminal_at, id)',
 'direction':'(session_slot, terminal_at DESC)',
 'invisible':'(session_slot, terminal_at) INVISIBLE',
 'unique':'UNIQUE (session_slot, terminal_at)',
}.items():
    ddl=f'CREATE INDEX idx_turn_retention_terminal ON conversation_turns {definition};'
    if name=='unique': ddl='CREATE UNIQUE INDEX idx_turn_retention_terminal ON conversation_turns (session_slot, terminal_at);'
    (root/f'tamper-{name}.sql').write_text(ddl+'\n')
PY
    for charset in latin1 utf8mb4 utf8mb3; do
        db="sec2_s1_up_$charset"
        mysql_root -e "CREATE DATABASE $db" </dev/null || return $?
        mysql_root "$db" < "$dir/base.sql" > "$dir/$db.log" 2>&1 || return $?
        mysql_root "$db" < "$repo_root/scripts/fixtures/ticket07_populate.sql" >> "$dir/$db.log" 2>&1 || return $?
        local before_rows after_rows
        before_rows="$(mysql_root -N -B "$db" -e 'SELECT COUNT(*) FROM conversation_turns; SELECT COUNT(*) FROM consumo_llm' </dev/null)"
        for repeat in 1 2; do
            mysql_root --default-character-set="$charset" "$db" < "$repo_root/db/init.sql" >> "$dir/$db.log" 2>&1 || { tail -30 "$dir/$db.log"; return 1; }
        done
        after_rows="$(mysql_root -N -B "$db" -e 'SELECT COUNT(*) FROM conversation_turns; SELECT COUNT(*) FROM consumo_llm' </dev/null)"
        [[ "$before_rows" == "$after_rows" ]] || return 1
        total_up=$((total_up+1))
        for cut in 0 1; do
            db="sec2_s1_int_${charset}_$cut"
            mysql_root -e "CREATE DATABASE $db" </dev/null || return $?
            mysql_root "$db" < "$dir/base.sql" > "$dir/$db.log" 2>&1 || return $?
            mysql_root --default-character-set="$charset" "$db" < "$dir/int-$cut.sql" >> "$dir/$db.log" 2>&1 || return $?
            mysql_root "$db" < "$repo_root/db/init.sql" >> "$dir/$db.log" 2>&1 || return $?
            total_int=$((total_int+1))
        done
        for tamper in "$dir"/tamper-*.sql; do
            db="sec2_s1_${charset}_$(basename "$tamper" .sql | tr - _)"
            mysql_root -e "CREATE DATABASE $db" </dev/null || return $?
            mysql_root "$db" < "$dir/base.sql" > "$dir/$db.log" 2>&1 || return $?
            mysql_root "$db" < "$tamper" >> "$dir/$db.log" 2>&1 || return $?
            local before after
            before="$(mysql_root -N -B "$db" -e 'SHOW CREATE TABLE conversation_turns' </dev/null)"
            if mysql_root "$db" < "$repo_root/db/init.sql" >> "$dir/$db.log" 2>&1; then echo "SEC2_S1_TAMPER FAIL $db"; return 1; fi
            grep -q 'sec2-s1 retention index shape requires audited repair' "$dir/$db.log" || return 1
            after="$(mysql_root -N -B "$db" -e 'SHOW CREATE TABLE conversation_turns' </dev/null)"
            [[ "$before" == "$after" ]] || return 1
            total_tamper=$((total_tamper+1))
            if [[ "$tamper" == *tamper-direction.sql ]]; then
                # Removing only the exact-shape guard accepts the bad index:
                # the unmutated validator above rejected this same real DB.
                mysql_root "$db" < "$dir/mutant.sql" >> "$dir/$db.log" 2>&1 || return $?
                total_mut=$((total_mut+1))
            fi
        done
    done
    [[ "$total_up,$total_int,$total_tamper,$total_mut" == 3,6,18,3 ]] || return 1
    echo "SEC2_S1_SCHEMA PASS upgrades=$total_up interruptions=$total_int tampers=$total_tamper validator_mutations=$total_mut"
}
sec2_s1_schema_matrix
