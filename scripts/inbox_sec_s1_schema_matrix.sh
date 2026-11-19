#!/usr/bin/env bash
# S1: sourced by T082; uses its isolated server, mysql_root, paths, logs.
# Every schema is owned by that disposable server. No external resources.
sec_s1_schema_matrix() {
    local charset db ref fixture before after total_up=0 total_int=0 total_tamper=0
    local dir="${workdir}/s1-schema"
    mkdir -p "$dir"
    python - "$repo_root/db/init.sql" "$dir" <<'PY' || return $?
from pathlib import Path
import sys
sql = Path(sys.argv[1]).read_text(); root=Path(sys.argv[2])
start=sql.index('-- S1: durable retention checkpoint.')
suffix=sql[start:]
(root/'s1.sql').write_text(suffix)
# Interruption immediately after CREATE, validation, seed, version bump.
markers=['progress-created','progress-validated','progress-seeded','schema-version-recorded']
for i, marker in enumerate(markers):
    boundary='-- sec-s1-schema-boundary: '+marker
    (root/f'int-{i}.sql').write_text(suffix[:suffix.index(boundary)])
a=suffix.index('CREATE TABLE IF NOT EXISTS scheduler_retention_progress (')
b=suffix.index(');',a)+2
ddl=suffix[a:b]
cases={
 'type': ('last_turn_id BIGINT','last_turn_id INT'),
 'signed': ('last_turn_id BIGINT UNSIGNED','last_turn_id BIGINT'),
 'null': ('last_turn_id BIGINT UNSIGNED NOT NULL','last_turn_id BIGINT UNSIGNED NULL'),
 'default': ('version BIGINT UNSIGNED NOT NULL','version BIGINT UNSIGNED NOT NULL DEFAULT 0'),
 'order': ('last_turn_id BIGINT UNSIGNED NOT NULL,\n    version BIGINT UNSIGNED NOT NULL',
           'version BIGINT UNSIGNED NOT NULL,\n    last_turn_id BIGINT UNSIGNED NOT NULL'),
 'extra': ('id INT NOT NULL','id INT NOT NULL AUTO_INCREMENT'),
 'missing': ('    version BIGINT UNSIGNED NOT NULL,\n',''),
 'charset': ('last_turn_id BIGINT UNSIGNED NOT NULL','last_turn_id VARCHAR(30) CHARACTER SET ascii COLLATE ascii_bin NOT NULL'),
 'pk': ('PRIMARY KEY (id)','PRIMARY KEY (id, version)'),
 'index': ('PRIMARY KEY (id)','PRIMARY KEY (id), INDEX idx_extra (version)'),
 'check': ('PRIMARY KEY (id)','PRIMARY KEY (id), CONSTRAINT ck_s1_extra CHECK (id = 1)'),
 'trigger': ('', ''),
 'fk': ('', ''),
}
for name,(old,new) in cases.items():
    value=ddl.replace(old,new) if old else ddl
    if name=='trigger': value+='\nCREATE TRIGGER sec_s1_extra BEFORE UPDATE ON scheduler_retention_progress FOR EACH ROW SET NEW.id=NEW.id;'
    if name=='fk': value+='\nCREATE TABLE sec_s1_child (id INT PRIMARY KEY, CONSTRAINT fk_sec_s1_child FOREIGN KEY(id) REFERENCES scheduler_retention_progress(id));'
    (root/f'tamper-{name}.sql').write_text(value+'\n')
PY
    for charset in latin1 utf8mb4 utf8mb3; do
        while read -r -u 4 ref fixture; do
            db="sec_s1_up_${ref}_${charset}"
            git -C "$repo_root" show "${ref}:db/init.sql" > "$dir/base.sql" || return $?
            mysql_root --execute="CREATE DATABASE $db" </dev/null || return $?
            mysql_root --default-character-set=latin1 "$db" < "$dir/base.sql" > "$dir/$db.log" 2>&1 || return $?
            mysql_root "$db" < "$repo_root/scripts/fixtures/$fixture" >> "$dir/$db.log" 2>&1 || return $?
            before="$(mysql_root -N -B "$db" -e 'SELECT COUNT(*) FROM conversation_turns; SELECT COUNT(*) FROM consumo_llm;' </dev/null)"
            for repeat in 1 2; do
                mysql_root --default-character-set="$charset" "$db" < "$repo_root/db/init.sql" >> "$dir/$db.log" 2>&1 || { tail -30 "$dir/$db.log"; return 1; }
            done
            after="$(mysql_root -N -B "$db" -e 'SELECT COUNT(*) FROM conversation_turns; SELECT COUNT(*) FROM consumo_llm;' </dev/null)"
            [[ "$before" == "$after" ]] || return 1
            [[ "$(mysql_root -N -B "$db" -e 'SELECT schema_version FROM scheduler_runtime_state; SELECT CONCAT_WS(",",id,last_turn_id,version) FROM scheduler_retention_progress' </dev/null)" == $'3\n1,0,0' ]] || return 1
            # Existing progress and future schema marker must NEVER be reset.
            mysql_root "$db" -e 'UPDATE scheduler_retention_progress SET last_turn_id=987,version=77; UPDATE scheduler_runtime_state SET schema_version=4' </dev/null || return $?
            mysql_root --default-character-set="$charset" "$db" < "$repo_root/db/init.sql" >> "$dir/$db.log" 2>&1 || return $?
            [[ "$(mysql_root -N -B "$db" -e 'SELECT schema_version FROM scheduler_runtime_state; SELECT CONCAT_WS(",",id,last_turn_id,version) FROM scheduler_retention_progress' </dev/null)" == $'4\n1,987,77' ]] || return 1
            echo "SEC_S1_UPGRADE PASS $ref $charset populated repeated future-marker checkpoint-preserved"
            total_up=$((total_up+1))
        done 4<<'REFS'
a961a1e ticket07_populate.sql
1a7e1ed ticket06_populate.sql
f59c32c ticket05_populate.sql
d52a495 ticket05_populate.sql
35f9c07 ticket05_populate.sql
148dd57 ticket07_populate.sql
52c6aac ticket07_populate.sql
61a9f73 ticket07_populate.sql
REFS
        for cut in 0 1 2 3; do
            db="sec_s1_int_${charset}_${cut}"
            mysql_root --execute="CREATE DATABASE $db" </dev/null || return $?
            # base.sql is 61a9f73 with marker=2 and no new table.
            mysql_root --default-character-set=latin1 "$db" < "$dir/base.sql" > "$dir/$db.log" 2>&1 || return $?
            mysql_root --default-character-set="$charset" "$db" < "$dir/int-$cut.sql" >> "$dir/$db.log" 2>&1 || return $?
            for repeat in 1 2; do
                mysql_root --default-character-set="$charset" "$db" < "$repo_root/db/init.sql" >> "$dir/$db.log" 2>&1 || return $?
            done
            [[ "$(mysql_root -N -B "$db" -e 'SELECT schema_version FROM scheduler_runtime_state; SELECT CONCAT_WS(",",id,last_turn_id,version) FROM scheduler_retention_progress' </dev/null)" == $'3\n1,0,0' ]] || return 1
            echo "SEC_S1_INTERRUPTION PASS $charset $cut"
            total_int=$((total_int+1))
        done
        for tamper in "$dir"/tamper-*.sql; do
            name="$(basename "$tamper" .sql)"
            db="sec_s1_${charset}_${name//-/_}"
            mysql_root --execute="CREATE DATABASE $db" </dev/null || return $?
            mysql_root --default-character-set=latin1 "$db" < "$dir/base.sql" > "$dir/$db.log" 2>&1 || return $?
            mysql_root --default-character-set="$charset" "$db" < "$tamper" >> "$dir/$db.log" 2>&1 || return $?
            before="$(mysql_root -N -B "$db" -e 'SHOW CREATE TABLE scheduler_retention_progress' </dev/null)"
            if mysql_root --default-character-set="$charset" "$db" < "$repo_root/db/init.sql" >> "$dir/$db.log" 2>&1; then
                echo "SEC_S1_TAMPER FAIL $name accepted"; return 1
            fi
            grep -qE 'sec-s1 retention (column|index|dependency) shape requires audited repair' "$dir/$db.log" || { tail -20 "$dir/$db.log"; return 1; }
            after="$(mysql_root -N -B "$db" -e 'SHOW CREATE TABLE scheduler_retention_progress' </dev/null)"
            [[ "$before" == "$after" ]] || return 1
            echo "SEC_S1_TAMPER PASS $charset $name fail-closed no-rewrite"
            total_tamper=$((total_tamper+1))
        done
    done
    [[ "$total_up/$total_int/$total_tamper" == '24/12/39' ]] || return 1
    echo "SEC_S1_SCHEMA PASS upgrades=$total_up interruptions=$total_int tampers=$total_tamper"
}
sec_s1_schema_matrix
