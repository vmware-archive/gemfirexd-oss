#!/usr/bin/env bash
#
# Auto completion script for GemFireXD's gfxd script
#
# Either explicitly source it into your shell enviroment, set it up in your
# .bashrc or .bash_profile or copy it to /etc/bash_completion.d


_gfxd() {
    # The utilites provided by gfxd
    declare -r UTILITIES="server locator agent version stats \
        merge-logs encrypt-password validate-disk-store \
        upgrade-disk-store compact-disk-store compact-all-disk-stores \
        revoke-missing-disk-store list-missing-disk-stores modify-disk-store \
        show-disk-store-metadata export-disk-store shut-down-all \
        backup print-stacks install-jar replace-jar remove-jar \
        run write-schema-to-xml write-schema-to-sql write-data-to-xml \
        write-data-dtd-to-file write-schema-to-db write-data-to-db replay-failed-dmls"

    # The verbs relevant to the utilities
    declare -r V_server="start stop status wait"
    declare -r V_locator="start stop status wait"
    declare -r V_agent="start stop status wait"

    # These utilities don't have options - must have a space here though
    declare -r OPT_version=" "
    declare -r OPT_stats=" "
    declare -r OPT_merge_logs=" "
    declare -r OPT_validate_disk_store=" "
    declare -r OPT_upgrade_disk_store=" "
    declare -r OPT_compact_disk_store=" "

    # The options relevant to each utility-verb combination
    declare -r OPT_server_start="-J -dir -classpath -sync -heap-size \
        -off-heap-size -mcast-port -mcast-address -locators -start-locator \
        -server-groups -lock-memory -rebalance -init-scripts -bind-address \
        -run-netserver -client-bind-address -client-port \
        -critical-heap-percentage -eviction-heap-percentage \
        -critical-off-heap-percentage -eviction-off-heap-percentage -host-data \
        -log-file -auth-provider -server-auth-provider -user -password"
    declare -r OPT_server_stop="-dir"
    declare -r OPT_server_status="-dir"
    declare -r OPT_server_wait="-dir"

    declare -r OPT_locator_start="-J -dir -classpath -heap-size \
        -peer-discovery-address -peer-discovery-port -sync -bind-address \
        -run-netserver -client-bind-address -client-port -locators -log-file \
        -auth-provider -server-auth-provider -user -password"
    declare -r OPT_locator_stop="-dir"
    declare -r OPT_locator_status="-dir"
    declare -r OPT_locator_wait="-dir"

    declare -r OPT_agent_start="-J -dir -classpath -mcast-port -mcast-address \
        -locators -bind-address -log-file -auth-provider -server-auth-provider \
        -user -password"
    declare -r OPT_agent_stop="-dir"
    declare -r OPT_agent_status="-dir"
    declare -r OPT_agent_wait="-dir"

    declare -r OPT_encrypt_password="-transformation -keysize -J-D \
        -mcast-port -mcast-address -locators -bind-address"

    declare -r OPT_compact_all_disk_stores="-J-D \
        -mcast-port -mcast-address -locators -bind-address"

    declare -r OPT_revoke_missing_disk_store="-J-D \
        -mcast-port -mcast-address -locators -bind-address"

    declare -r OPT_list_missing_disk_stores="-J-D \
        -mcast-port -mcast-address -locators -bind-address"

    declare -r OPT_modify_disk_store="-region -remove -lru -lruAction \
        -lruLimit -initialCapacity -loadFactor -statisticsEnabled"

    declare -r OPT_show_disk_store_metadata="-region -remove -lru -lruAction \
        -lruLimit -concurrencyLevel -initialCapacity -loadFactor \
        statisticsEnabled"

    declare -r OPT_export_disk_store="-bucket"

    declare -r OPT_shut_down_all="-J-D -mcast-port -mcast-address \
        -locators -bind-address"

    declare -r OPT_backup="-J-D -mcast-port -mcast-address \
        -locators -bind-address"

    declare -r OPT_print_stacks="-J-D -mcast-port -mcast-address \
        -locators -bind-address"

    declare -r OPT_install_jar="-file -name -auth-provider -bind-address \
        -client-bind-address -client-port -extra-conn-props -help --help \
        -J-D -locators -mcast-address -mcast-port -password -user"
    declare -r OPT_replace_jar="-file -name -auth-provider -bind-address \
        -client-bind-address -client-port -extra-conn-props -help --help \
        -J-D -locators -mcast-address -mcast-port -password -user"
    declare -r OPT_remove_jar="-file -name -auth-provider -bind-address \
        -client-bind-address -client-port -extra-conn-props -help --help \
        -J-D -locators -mcast-address -mcast-port -password -user"

    declare -r OPT_run="-file -auth-provider -bind-address \
        -client-bind-address -client-port -encoding -extra-conn-props \
        -help --help -ignore-errors -J-D -locators -mcast-address \
        -mcast-port -password -path -user"

    declare -r OPT_write_schema_to_xml="-file -auth-provider -bind-address \
        -catalog-pattern -client-bind-address -client-port -database-type \
        -delimited-identifiers -driver-class -exclude-table-filter \
        -exclude-tables -extra-conn-props -help --help -include-table-filter \
        -include-tables -isolation-level -J-D -locators -mcast-address \
        -mcast-port -password -schema-pattern -url -user -verbose"

    declare -r OPT_write_schema_to_sql="$OPT_write_schema_to_xml \
        -export-all -generic -to-database-type -xml-schema-files"

    declare -r OPT_write_schema_to_db="-files -alter-identity-columns \
        -auth-provider -bind-address -catalog-pattern \
        -catalog-pattern -client-bind-address -client-port -database-type \
        -delimited-identifiers -do-drops -driver-class \
        -extra-conn-props -help --help -J-D -locators -mcast-address \
        -mcast-port -password -schema-pattern -url -user -verbose"

    declare -r OPT_write_data_to_xml="-file -auth-provider -bind-address \
        -catalog-pattern -client-bind-address -client-port -database-type \
        -delimited-identifiers -driver-class -exclude-table-filter \
        -exclude-tables -extra-conn-props -help --help -include-table-filter \
        -include-tables -isolation-level -J-D -locators -mcast-address \
        -mcast-port -password -schema-pattern -url -user -verbose"

    declare -r OPT_write_data_to_db="-files -alter-identity-columns \
        -auth-provider -batch-size -bind-address \
        -catalog-pattern -client-bind-address -client-port -database-type \
        -delimited-identifiers -driver-class -ensure-fk-order \
        -extra-conn-props -help --help -J-D -locators -mcast-address \
        -mcast-port -password -schema-files -schema-pattern -url -user -verbose"

    declare -r OPT_write_data_dtd_to_file="-file -auth-provider -bind-address \
        -client-bind-address -client-port -driver-class -extra-conn-props \
        -help --help -J-D -locators -mcast-address -mcast-port -password -url \
        -user -verbose"

    declare -r OPT_replay_failed_dmls="-file -auth-provider -bind-address \
        -client-bind-address -client-port -driver-class -extra-conn-props \
        -help --help -J-D -locators -mcast-address -mcast-port -password -url \
        -user -verbose"

    local cur=${COMP_WORDS[COMP_CWORD]}
    local use="UTILITIES"

    local utility=${COMP_WORDS[1]}
    local verb=${COMP_WORDS[2]}

    # Ignore potential options
    verb=${verb##-*}

    # Because variable names can't have dashes
    utility=${utility//-/_}
    verb=${verb//-/_}

    if [[ -n "$verb" ]]; then
        use="OPT_${utility}_${verb}"
        if [[ -z "${!use}" ]]; then
            use="V_$utility"
        fi
    elif [[ -n "$utility" ]]; then
        use="OPT_$utility"
        if [[ -z "${!use}" ]]; then
            use="V_$utility"
            if [[ -z "${!use}" ]]; then
                use="UTILITIES"
            fi
        fi
    fi

    COMPREPLY=( $( compgen -W "${!use}" -- "$cur" ) )
}

complete -F _gfxd gfxd
    
