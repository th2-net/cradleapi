#!/bin/bash

# Copyright 2024-2025 Exactpro (Exactpro Systems Limited)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Exceptions:
#   session_statistics table is missed because this table doesn't contain useful information
#   example:
#     SELECT * FROM session_statistics LIMIT 1;
#
#      book      | page                    | record_type | frame_type | frame_start                     | session
#     -----------+-------------------------+-------------+------------+---------------------------------+------------
#      test_book | auto-page-1711924200000 |           2 |          1 | 2024-03-31 22:30:04.600000+0000 | demo-conn

# FIXME: add start / end parameters

SCRIPT_PATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 || exit ; pwd -P )"

DIR_DATA_NAME='data'
DIR_DATA="${SCRIPT_PATH}/${DIR_DATA_NAME}"
DIR_ANALYTICS_NAME='analytics'
DIR_ANALYTICS="${SCRIPT_PATH}/${DIR_ANALYTICS_NAME}"
DIR_LOGS_NAME='logs'
DIR_LOGS="${SCRIPT_PATH}/${DIR_LOGS_NAME}"

DOWNLOAD_MODE_ALL='all'
DOWNLOAD_MODE_BOOKS='books'
DOWNLOAD_MODE_PAGES='pages'
DOWNLOAD_MODE_SESSIONS='sessions'
DOWNLOAD_MODE_GROUPS='groups'
DOWNLOAD_MODE_SCOPES='scopes'
DOWNLOAD_MODE_MESSAGE_STATISTICS='message-statistics'
DOWNLOAD_MODE_MESSAGE_ENTITY_STATISTICS='message-entity-statistics'
DOWNLOAD_MODE_EVENT_ENTITY_STATISTICS='event-entity-statistics'
DOWNLOAD_MODE_GROUPED_MESSAGES='grouped-messages'
DOWNLOAD_MODE_TEST_EVENTS='test-events'

ARG_CASSANDRA_REQUEST_TIMEOUT='--cassandra-request-timeout'
ARG_CASSANDRA_CONNECT_TIMEOUT='--cassandra-connect-timeout'
ARG_CASSANDRA_HOST='--cassandra-host'
ARG_CASSANDRA_KEYSPACE='--cassandra-keyspace'
ARG_CASSANDRA_USERNAME='--cassandra-username'
ARG_CASSANDRA_PASSWORD='--cassandra-password'
ARG_DOWNLOAD_MODE='--download-mode'
ARG_BOOKS_CSV="--${DOWNLOAD_MODE_BOOKS}-csv"
ARG_PAGES_CSV="--${DOWNLOAD_MODE_PAGES}-csv"
ARG_SESSIONS_CSV="--${DOWNLOAD_MODE_SESSIONS}-csv"
ARG_GROUPS_CSV="--${DOWNLOAD_MODE_GROUPS}-csv"
ARG_SCOPES_CSV="--${DOWNLOAD_MODE_SCOPES}-csv"
ARG_MESSAGE_STATISTICS_CSV="--${DOWNLOAD_MODE_MESSAGE_STATISTICS}-csv"
ARG_MESSAGE_ENTITY_STATISTICS_CSV="--${DOWNLOAD_MODE_MESSAGE_ENTITY_STATISTICS}-csv"
ARG_EVENT_ENTITY_STATISTICS_CSV="--${DOWNLOAD_MODE_EVENT_ENTITY_STATISTICS}-csv"
ARG_GROUPED_MESSAGES_CSV="--${DOWNLOAD_MODE_GROUPED_MESSAGES}-csv"
ARG_TEST_EVENTS_CSV="--${DOWNLOAD_MODE_TEST_EVENTS}-csv"
ARG_CQLSH_LOG='--cqlsh-log'

DEFAULT_BOOKS_CSV="${DOWNLOAD_MODE_BOOKS}.csv"
DEFAULT_PAGES_CSV="${DOWNLOAD_MODE_PAGES}.csv"
DEFAULT_SESSIONS_CSV="${DOWNLOAD_MODE_SESSIONS}.csv"
DEFAULT_GROUPS_CSV="${DOWNLOAD_MODE_GROUPS}.csv"
DEFAULT_SCOPES_CSV="${DOWNLOAD_MODE_SCOPES}.csv"
DEFAULT_MESSAGE_STATISTICS_CSV="${DOWNLOAD_MODE_MESSAGE_STATISTICS}.csv"
DEFAULT_MESSAGE_ENTITY_STATISTICS_CSV="${DOWNLOAD_MODE_MESSAGE_ENTITY_STATISTICS}.csv"
DEFAULT_EVENT_ENTITY_STATISTICS_CSV="${DOWNLOAD_MODE_EVENT_ENTITY_STATISTICS}.csv"
DEFAULT_GROUPED_MESSAGES_CSV="${DOWNLOAD_MODE_GROUPED_MESSAGES}.csv"
DEFAULT_TEST_EVENTS_CSV="${DOWNLOAD_MODE_TEST_EVENTS}.csv"
DEFAULT_CQLSH_LOG='cqlsh.log'

CRADLE_DIRECTIONS=('1' '2')
CRADLE_MESSAGE_ENTITY_TYPE=1
CRADLE_EVENT_ENTITY_TYPE=2
CRADLE_STATISTIC_FRAME_HOUR=4

QUERIES_BATCH_SIZE=1000

init() {
  mkdir -p "${DIR_DATA}"
  mkdir -p "${DIR_ANALYTICS}"
  mkdir -p "${DIR_LOGS}"
  truncate -s 0 "${DIR_LOGS}/${FILE_CQLSH_LOG}"
}

download_mode_help() {
  echo ' Download:'
  echo "  Arg: ${ARG_DOWNLOAD_MODE} - one of values:"
  echo "    * ${DOWNLOAD_MODE_BOOKS} - download books into ${DIR_DATA_NAME}/${FILE_BOOKS_CSV}"
  echo "    * ${DOWNLOAD_MODE_PAGES} - download pages for each books defined in ${DIR_DATA_NAME}/${FILE_BOOKS_CSV} into ${DIR_DATA_NAME}/${FILE_PAGES_CSV}"
  echo "    * ${DOWNLOAD_MODE_SESSIONS} - download sessions for each books defined in ${DIR_DATA_NAME}/${FILE_BOOKS_CSV} into ${DIR_DATA_NAME}/${FILE_SESSIONS_CSV}"
  echo "    * ${DOWNLOAD_MODE_GROUPS} - download session groups for each books defined in ${DIR_DATA_NAME}/${FILE_BOOKS_CSV} into ${DIR_DATA_NAME}/${FILE_GROUPS_CSV}"
  echo "    * ${DOWNLOAD_MODE_SCOPES} - download scopes for each books defined in ${DIR_DATA_NAME}/${FILE_BOOKS_CSV} into ${DIR_DATA_NAME}/${FILE_SCOPES_CSV}"
  echo "    * ${DOWNLOAD_MODE_MESSAGE_STATISTICS} - calculate statistics by message_statistics table, write into ${DIR_ANALYTICS_NAME}/${FILE_MESSAGE_STATISTICS_CSV}. Use data for calculation:"
  echo "      - pages defined in ${DIR_DATA_NAME}/${FILE_PAGES_CSV}"
  echo "      - sessions defined in ${DIR_DATA_NAME}/${FILE_SESSIONS_CSV}"
  echo "    * ${DOWNLOAD_MODE_MESSAGE_ENTITY_STATISTICS} - calculate message statistics by entity_statistics table, write into ${DIR_ANALYTICS_NAME}/${FILE_MESSAGE_ENTITY_STATISTICS_CSV}. Use data for calculation:"
  echo "      - pages defined in ${DIR_DATA_NAME}/${FILE_PAGES_CSV}"
  echo "    * ${DOWNLOAD_MODE_EVENT_ENTITY_STATISTICS} - calculate events statistics by entity_statistics table, write into ${DIR_ANALYTICS_NAME}/${FILE_EVENT_ENTITY_STATISTICS_CSV}. Use data for calculation:"
  echo "      - pages defined in ${DIR_DATA_NAME}/${FILE_PAGES_CSV}"
  echo "    * ${DOWNLOAD_MODE_GROUPED_MESSAGES} - calculate message statistics by grouped_messages table, write into ${DIR_ANALYTICS_NAME}/${FILE_GROUPED_MESSAGES_CSV}. Use data for calculation:"
  echo "      - pages defined in ${DIR_DATA_NAME}/${FILE_PAGES_CSV}"
  echo "      - groups defined in ${DIR_DATA_NAME}/${FILE_GROUPS_CSV}"
  echo "    * ${DOWNLOAD_MODE_TEST_EVENTS} - calculate events statistics by test_events table, write into ${DIR_ANALYTICS_NAME}/${FILE_TEST_EVENTS_CSV}. Use data for calculation:"
  echo "      - pages defined in ${DIR_DATA_NAME}/${FILE_PAGES_CSV}"
  echo "      - scopes defined in ${DIR_DATA_NAME}/${FILE_SCOPES_CSV}"
}

print_help() {
  echo 'Help:'
  echo ' Cassandra:'
  echo "  Arg: ${ARG_CASSANDRA_REQUEST_TIMEOUT} (optional) - specify the default request timeout in seconds"
  echo "  Arg: ${ARG_CASSANDRA_CONNECT_TIMEOUT} (optional) - specify the connection timeout in seconds"
  echo "  Arg: ${ARG_CASSANDRA_HOST} (optional) - cassandra host"
  echo "  Arg: ${ARG_CASSANDRA_KEYSPACE} (required) - cassandra keyspace"
  echo "  Arg: ${ARG_CASSANDRA_USERNAME} (optional) - authenticate as user"
  echo "  Arg: ${ARG_CASSANDRA_PASSWORD} (optional) - authenticate using password"
  download_mode_help
  echo ' Data files:'
  echo "  Arg: ${ARG_BOOKS_CSV} (optional, default is '${DEFAULT_BOOKS_CSV}') - file name for storing / using books information in ${DIR_DATA_NAME} dir"
  echo "  Arg: ${ARG_PAGES_CSV} (optional, default is '${DEFAULT_PAGES_CSV}') - file name for storing / using pages information in ${DIR_DATA_NAME} dir"
  echo "  Arg: ${ARG_SESSIONS_CSV} (optional, default is '${DEFAULT_SESSIONS_CSV}') - file name for storing / using sessions information in ${DIR_DATA_NAME} dir"
  echo "  Arg: ${ARG_GROUPS_CSV} (optional, default is '${DEFAULT_GROUPS_CSV}') - file name for storing / using session groups information in ${DIR_DATA_NAME} dir"
  echo "  Arg: ${ARG_SCOPES_CSV} (optional, default is '${DEFAULT_SCOPES_CSV}') - file name for storing / using scopes information in ${DIR_DATA_NAME} dir"
  echo ' Analytics files:'
  echo "  Arg: ${ARG_MESSAGE_STATISTICS_CSV} (optional, default is '${DEFAULT_MESSAGE_STATISTICS_CSV}') - file name for storing session information in ${DIR_ANALYTICS_NAME} dir"
  echo "  Arg: ${ARG_MESSAGE_ENTITY_STATISTICS_CSV} (optional, default is '${DEFAULT_MESSAGE_ENTITY_STATISTICS_CSV}') - file name for storing messages information in ${DIR_ANALYTICS_NAME} dir"
  echo "  Arg: ${ARG_EVENT_ENTITY_STATISTICS_CSV} (optional, default is '${DEFAULT_EVENT_ENTITY_STATISTICS_CSV}') - file name for storing events information in ${DIR_ANALYTICS_NAME} dir"
  echo "  Arg: ${ARG_GROUPED_MESSAGES_CSV} (optional, default is '${DEFAULT_GROUPED_MESSAGES_CSV}') - file name for storing grouped message information in ${DIR_ANALYTICS_NAME} dir"
  echo "  Arg: ${ARG_TEST_EVENTS_CSV} (optional, default is '${DEFAULT_TEST_EVENTS_CSV}') - file name for storing test events information in ${DIR_ANALYTICS_NAME} dir"
  echo ' Log files:'
  echo "  Arg: ${ARG_CQLSH_LOG} (optional, default is '${DEFAULT_CQLSH_LOG}') - file name for writing system output / error from cqlsh in ${DIR_LOGS} dir"
}

parse_args() {
  DOWNLOAD_MODE=''
  FILE_BOOKS_CSV="${DEFAULT_BOOKS_CSV}"
  FILE_PAGES_CSV="${DEFAULT_PAGES_CSV}"
  FILE_SESSIONS_CSV="${DEFAULT_SESSIONS_CSV}"
  FILE_GROUPS_CSV="${DEFAULT_GROUPS_CSV}"
  FILE_SCOPES_CSV="${DEFAULT_SCOPES_CSV}"
  FILE_MESSAGE_STATISTICS_CSV="${DEFAULT_MESSAGE_STATISTICS_CSV}"
  FILE_MESSAGE_ENTITY_STATISTICS_CSV="${DEFAULT_MESSAGE_ENTITY_STATISTICS_CSV}"
  FILE_EVENT_ENTITY_STATISTICS_CSV="${DEFAULT_EVENT_ENTITY_STATISTICS_CSV}"
  FILE_GROUPED_MESSAGES_CSV="${DEFAULT_GROUPED_MESSAGES_CSV}"
  FILE_TEST_EVENTS_CSV="${DEFAULT_TEST_EVENTS_CSV}"
  FILE_CQLSH_LOG="${DEFAULT_CQLSH_LOG}"
  CASSANDRA_KEYSPACE=''
  CASSANDRA_REQUEST_TIMEOUT=10
  CASSANDRA_CONNECT_TIMEOUT=5
  CASSANDRA_HOST='127.0.0.1'
  CASSANDRA_USERNAME=''
  CASSANDRA_PASSWORD=''

  while [[ "$#" -gt 0 ]]; do
    case "$1" in
      --help)
        print_help
        exit 0
        ;;
      "${ARG_DOWNLOAD_MODE}")
        DOWNLOAD_MODE="$2"
        shift 2
        ;;
      "${ARG_BOOKS_CSV}")
        FILE_BOOKS_CSV="$2"
        shift 2
        ;;
      "${ARG_PAGES_CSV}")
        FILE_PAGES_CSV="$2"
        shift 2
        ;;
      "${ARG_SESSIONS_CSV}")
        FILE_SESSIONS_CSV="$2"
        shift 2
        ;;
      "${ARG_GROUPS_CSV}")
        FILE_GROUPS_CSV="$2"
        shift 2
        ;;
      "${ARG_SCOPES_CSV}")
        FILE_SCOPES_CSV="$2"
        shift 2
        ;;
      "${ARG_MESSAGE_STATISTICS_CSV}")
        FILE_MESSAGE_STATISTICS_CSV="$2"
        shift 2
        ;;
      "${ARG_MESSAGE_ENTITY_STATISTICS_CSV}")
        FILE_MESSAGE_ENTITY_STATISTICS_CSV="$2"
        shift 2
        ;;
      "${ARG_EVENT_ENTITY_STATISTICS_CSV}")
        FILE_EVENT_ENTITY_STATISTICS_CSV="$2"
        shift 2
        ;;
      "${ARG_GROUPED_MESSAGES_CSV}")
        FILE_GROUPED_MESSAGES_CSV="$2"
        shift 2
        ;;
      "${ARG_TEST_EVENTS_CSV}")
        FILE_TEST_EVENTS_CSV="$2"
        shift 2
        ;;
      "${ARG_CQLSH_LOG}")
        FILE_CQLSH_LOG="$2"
        shift 2
        ;;
      "${ARG_CASSANDRA_KEYSPACE}")
        CASSANDRA_KEYSPACE="$2"
        shift 2
        ;;
      "${ARG_CASSANDRA_REQUEST_TIMEOUT}")
        CASSANDRA_REQUEST_TIMEOUT="$2"
        shift 2
        ;;
      "${ARG_CASSANDRA_CONNECT_TIMEOUT}")
        CASSANDRA_CONNECT_TIMEOUT="$2"
        shift 2
        ;;
      "${ARG_CASSANDRA_HOST}")
        CASSANDRA_HOST="$2"
        shift 2
        ;;
      "${ARG_CASSANDRA_USERNAME}")
        CASSANDRA_USERNAME="$2"
        shift 2
        ;;
      "${ARG_CASSANDRA_PASSWORD}")
        CASSANDRA_PASSWORD="$2"
        shift 2
        ;;
      *)
        echo "CRITICAL: Unknown option: $1"
        print_help
        exit 1
        ;;
    esac
  done

  if [ -z "${CASSANDRA_KEYSPACE}" ]; then
    echo "CRITICAL: Cassandra keyspace can't be empty, please specify ${ARG_CASSANDRA_KEYSPACE} argument"
    print_help
    exit 2
  fi

  echo 'Parameters:'
  echo ' Cassandra:'
  echo "  Arg: ${ARG_CASSANDRA_REQUEST_TIMEOUT} = ${CASSANDRA_REQUEST_TIMEOUT}"
  echo "  Arg: ${ARG_CASSANDRA_CONNECT_TIMEOUT} = ${CASSANDRA_CONNECT_TIMEOUT}"
  echo "  Arg: ${ARG_CASSANDRA_HOST} = ${CASSANDRA_HOST}"
  echo "  Arg: ${ARG_CASSANDRA_KEYSPACE} = ${CASSANDRA_KEYSPACE}"
  echo "  Arg: ${ARG_CASSANDRA_USERNAME} = ${CASSANDRA_USERNAME}"
  echo "  Arg: ${ARG_CASSANDRA_PASSWORD} = ***"
  echo ' Download:'
  echo "  Arg: ${ARG_DOWNLOAD_MODE} = ${DOWNLOAD_MODE}"
  echo ' Data files:'
  echo "  Arg: ${ARG_BOOKS_CSV} = ${FILE_BOOKS_CSV}"
  echo "  Arg: ${ARG_PAGES_CSV} = ${FILE_PAGES_CSV}"
  echo "  Arg: ${ARG_SESSIONS_CSV} = ${FILE_SESSIONS_CSV}"
  echo "  Arg: ${ARG_GROUPS_CSV} = ${FILE_GROUPS_CSV}"
  echo "  Arg: ${ARG_SCOPES_CSV} = ${FILE_SCOPES_CSV}"
  echo ' Analytics files:'
  echo "  Arg: ${ARG_MESSAGE_STATISTICS_CSV} = ${FILE_MESSAGE_STATISTICS_CSV}"
  echo "  Arg: ${ARG_MESSAGE_ENTITY_STATISTICS_CSV} = ${FILE_MESSAGE_ENTITY_STATISTICS_CSV}"
  echo "  Arg: ${ARG_EVENT_ENTITY_STATISTICS_CSV} = ${FILE_EVENT_ENTITY_STATISTICS_CSV}"
  echo "  Arg: ${ARG_GROUPED_MESSAGES_CSV} = ${FILE_GROUPED_MESSAGES_CSV}"
  echo "  Arg: ${ARG_TEST_EVENTS_CSV} = ${FILE_TEST_EVENTS_CSV}"
  echo ' Log files:'
  echo "  Arg: ${ARG_CQLSH_LOG} = ${FILE_CQLSH_LOG}"

  export DOWNLOAD_MODE="${DOWNLOAD_MODE}"
  export FILE_BOOKS_CSV="${FILE_BOOKS_CSV}"
  export FILE_PAGES_CSV="${FILE_PAGES_CSV}"
  export FILE_SESSIONS_CSV="${FILE_SESSIONS_CSV}"
  export FILE_GROUPS_CSV="${FILE_GROUPS_CSV}"
  export FILE_SCOPES_CSV="${FILE_SCOPES_CSV}"
  export FILE_MESSAGE_STATISTICS_CSV="${FILE_MESSAGE_STATISTICS_CSV}"
  export FILE_MESSAGE_ENTITY_STATISTICS_CSV="${FILE_MESSAGE_ENTITY_STATISTICS_CSV}"
  export FILE_EVENT_ENTITY_STATISTICS_CSV="${FILE_EVENT_ENTITY_STATISTICS_CSV}"
  export FILE_GROUPED_MESSAGES_CSV="${FILE_GROUPED_MESSAGES_CSV}"
  export FILE_TEST_EVENTS_CSV="${FILE_TEST_EVENTS_CSV}"
  export FILE_CQLSH_LOG="${FILE_CQLSH_LOG}"
  export CASSANDRA_KEYSPACE="${CASSANDRA_KEYSPACE}"
  export CASSANDRA_REQUEST_TIMEOUT="${CASSANDRA_REQUEST_TIMEOUT}"
  export CASSANDRA_CONNECT_TIMEOUT="${CASSANDRA_CONNECT_TIMEOUT}"
  export CASSANDRA_HOST="${CASSANDRA_HOST}"
  export CASSANDRA_USERNAME="${CASSANDRA_USERNAME}"
  export CASSANDRA_PASSWORD="${CASSANDRA_PASSWORD}"
}

check_command_execution_status() {
  local comment="${1}"
  local exit_code="${2}"
  if [ "${exit_code}" -eq 0 ]; then
    echo "INFO: ${comment} - success"
  else
    echo "CRITICAL: ${comment} - failure ($exit_code)"
    exit 30
  fi
}

copy_csv_header() {
  local source="${1}"
  local target="${2}"
  echo "INFO: copying CSV header from ${source} to ${target}" >&2
  head -n 1 "${source}" > "${target}"
}

copy_csv_body() {
  local source="${1}"
  local target="${2}"
  echo "INFO: copying CSV body from ${source} to ${target}" >&2
  local header
  header=$(head -1 "${target}")
  grep -v "${header}" "${source}" >> "${target}"
}

remove_file() {
  local file="${1}"
  echo "DEBUG: removing ${file}" >&2
  rm "${file}"
}

build_cassandra_command() {
  local parameter="${1}"
  local command

  command="cqlsh --keyspace '${CASSANDRA_KEYSPACE}' ${parameter}"
  if [ -n "${CASSANDRA_USERNAME}" ]; then
    command="${command} --username '${CASSANDRA_USERNAME}'"
  fi
  if [ -n "${CASSANDRA_PASSWORD}" ]; then
    command="${command} --password '${CASSANDRA_PASSWORD}'"
  fi
  if [ -n "${CASSANDRA_CONNECT_TIMEOUT}" ]; then
    command="${command} --connect-timeout ${CASSANDRA_CONNECT_TIMEOUT}"
  fi
  if [ -n "${CASSANDRA_REQUEST_TIMEOUT}" ]; then
    command="${command} --request-timeout ${CASSANDRA_REQUEST_TIMEOUT}"
  fi
  if [ -n "${CASSANDRA_HOST}" ]; then
    command="${command} '${CASSANDRA_HOST}'"
  fi
  echo "${command}"
}

build_cassandra_command_use_file() {
  local file="${1}"
  build_cassandra_command "--file \"${file}\""
}

build_cassandra_command_use_query() {
  local query="${1}"
  build_cassandra_command "--execute \"${query}\""
}

download_books() {
  echo "INFO: downloading books ..."
  m_start_0=$(date +%s%3N)
  command=$(build_cassandra_command_use_query "COPY books TO '${DIR_DATA}/${FILE_BOOKS_CSV}' WITH HEADER = TRUE;")
  eval "${command}" >> "${DIR_LOGS}/${FILE_CQLSH_LOG}" 2>&1
  exit_code=$?
  m_end_0=$(date +%s%3N)
  m_elapsed_0=$((m_end_0 - m_start_0))
  check_command_execution_status "downloaded books in ${m_elapsed_0} ms" "${exit_code}"
}

download_using_book() {
  local comment="${1}"
  local table_name="${2}"
  local output_file_name="${3}"

  echo "INFO: downloading ${comment} ..."
  m_start_0=$(date +%s%3N)
  header_written=false
  while IFS= read -r book; do
    temp_file=$(mktemp)
    echo "INFO: downloading ${comment} for ${book} book to ${temp_file} ..."
    m_start_1=$(date +%s%3N)
    command=$(build_cassandra_command_use_query "SELECT * FROM ${table_name} WHERE book='${book}';")
    eval "${command}" | grep "^ " | sed 's/^ *//; s/ *| */,/g' > "${temp_file}"
    exit_code=$?

    if [ "${exit_code}" -eq 0 ]; then
      if [ "${header_written}" = false ]; then
        copy_csv_header "${temp_file}" "${DIR_DATA}/${output_file_name}"
        header_written=true
      fi
      copy_csv_body "${temp_file}" "${DIR_DATA}/${output_file_name}"
    fi
    remove_file "${temp_file}"

    m_end_1=$(date +%s%3N)
    m_elapsed_1=$((m_end_1 - m_start_1))
    check_command_execution_status "downloaded ${comment} for ${book} book in ${m_elapsed_1} ms" "${exit_code}"
  done < <(tail -n +2 "${DIR_DATA}/${FILE_BOOKS_CSV}" | cut -d',' -f1)

  m_end_0=$(date +%s%3N)
  m_elapsed_0=$((m_end_0 - m_start_0))
  echo "INFO: downloaded ${comment} in ${m_elapsed_0} ms"
}

download_pages() {
  download_using_book "pages" "pages" "${FILE_PAGES_CSV}"
}

download_sessions() {
  download_using_book "sessions" "sessions" "${FILE_SESSIONS_CSV}"
}

download_groups() {
  download_using_book "groups" "groups" "${FILE_GROUPS_CSV}"
}

download_scopes() {
  download_using_book "scopes" "scopes" "${FILE_SCOPES_CSV}"
}

download_message_statistics() {
  echo "INFO: downloading message statistics ..."
  m_start_0=$(date +%s%3N)

  queries_temp_file=$(mktemp)
  while IFS= read -r page_line; do
    book=$(echo "${page_line}" | cut -d',' -f1 | sed 's/[\r\n]//g')
    page=$(echo "${page_line}" | cut -d',' -f7 | sed 's/[\r\n]//g')
    page_start_date=$(echo "${page_line}" | cut -d',' -f2 | sed 's/[\r\n]//g')
    page_start_time=$(echo "${page_line}" | cut -d',' -f3 | sed 's/[\r\n]//g')
    while IFS= read -r session_line; do
      session=$(echo "${session_line}" | cut -d',' -f2 | sed 's/[\r\n]//g')
      for direction in "${CRADLE_DIRECTIONS[@]}"; do
        echo "SELECT \
            book, page, session_alias, direction,
            MAX(frame_start) as max_frame_start, \
            SUM(entity_count) as sum_entity_count, \
            SUM(entity_size) as sum_entity_size \
          FROM message_statistics \
          WHERE book='${book}' AND \
            page='${page}' AND \
            session_alias='${session}' AND \
            direction='${direction}' AND \
            frame_type=${CRADLE_STATISTIC_FRAME_HOUR};" >> "${queries_temp_file}"
        echo "DEBUG: written query for ${book}/${page}/${session}/${direction} book/page/session/direction"
      done
    done < <(tail -n +2 "${DIR_DATA}/${FILE_SESSIONS_CSV}" | grep "^${book},")
  done < <(tail -n +2 "${DIR_DATA}/${FILE_PAGES_CSV}")

  execute_and_copy_result "${queries_temp_file}" 'null,null,null,null,null,0,0' "${DIR_ANALYTICS}/${FILE_MESSAGE_STATISTICS_CSV}" 'false' > /dev/null
  remove_file "${queries_temp_file}"

  m_end_0=$(date +%s%3N)
  m_elapsed_0=$((m_end_0 - m_start_0))
  echo "INFO: downloaded message statistics in ${m_elapsed_0} ms"
}

download_from_entity_statistics() {
  local comment="${1}"
  local entity_type="${2}"
  local output_file_name="${3}"

  echo "INFO: downloading ${comment} statistics ..."
  m_start_0=$(date +%s%3N)

  queries_temp_file=$(mktemp)
  while IFS= read -r page_line; do
    book=$(echo "${page_line}" | cut -d',' -f1 | sed 's/[\r\n]//g')
    page=$(echo "${page_line}" | cut -d',' -f7 | sed 's/[\r\n]//g')
    page_start_date=$(echo "${page_line}" | cut -d',' -f2 | sed 's/[\r\n]//g')
    page_start_time=$(echo "${page_line}" | cut -d',' -f3 | sed 's/[\r\n]//g')
    echo "SELECT \
            book, page, \
            MAX(frame_start) as max_frame_start, \
            SUM(entity_count) as sum_entity_count, \
            SUM(entity_size) as sum_entity_size \
          FROM entity_statistics \
          WHERE book='${book}' AND \
            page='${page}' AND \
            entity_type=${entity_type} AND \
            frame_type=${CRADLE_STATISTIC_FRAME_HOUR};" >> "${queries_temp_file}"
    echo "DEBUG: written query for ${comment} statistics for ${book}/${page} book/page ${page_start_date} ${page_start_time}"
  done < <(tail -n +2 "${DIR_DATA}/${FILE_PAGES_CSV}")

  execute_and_copy_result "${queries_temp_file}" 'null,null,null,0,0' "${DIR_ANALYTICS}/${output_file_name}" 'false' > /dev/null
  remove_file "${queries_temp_file}"

  m_end_0=$(date +%s%3N)
  m_elapsed_0=$((m_end_0 - m_start_0))
  echo "INFO: downloaded ${comment} statistics in ${m_elapsed_0} ms"
}

download_message_entity_statistics() {
  download_from_entity_statistics "message entity" "${CRADLE_MESSAGE_ENTITY_TYPE}" "${FILE_MESSAGE_ENTITY_STATISTICS_CSV}"
}

download_event_entity_statistics() {
  download_from_entity_statistics "event entity" "${CRADLE_EVENT_ENTITY_TYPE}" "${FILE_EVENT_ENTITY_STATISTICS_CSV}"
}

download_grouped_messages() {
  echo "INFO: downloading grouped message ..."
  m_start_0=$(date +%s%3N)

  header_written='false'
  queries_temp_file=$(mktemp)
  while IFS= read -r page_line; do
    book=$(echo "${page_line}" | cut -d',' -f1 | sed 's/[\r\n]//g')
    page=$(echo "${page_line}" | cut -d',' -f7 | sed 's/[\r\n]//g')
    page_start_date=$(echo "${page_line}" | cut -d',' -f2 | sed 's/[\r\n]//g')
    page_start_time=$(echo "${page_line}" | cut -d',' -f3 | sed 's/[\r\n]//g')
    while IFS= read -r group_line; do
      group=$(echo "${group_line}" | cut -d',' -f2 | sed 's/[\r\n]//g')
      echo "SELECT \
          book, page, alias_group, \
          MAX(rec_date) as max_rec_date, \
          SUM(message_count) as sum_message_count, \
          SUM(z_content_size) as sum_z_content_size, \
          SUM(z_content_uncompressed_size) as sum_z_content_uncompressed_size \
        FROM grouped_messages \
        WHERE book='${book}' AND \
          page='${page}' AND \
          alias_group='${group}';" >> "${queries_temp_file}"
      echo "DEBUG: written query for ${book}/${page}/${group} book/page/group"

      if [[ $(wc -l < "${queries_temp_file}") -ge "${QUERIES_BATCH_SIZE}" ]]; then
        header_written=$(execute_and_copy_result "${queries_temp_file}" 'null,null,null,null,0,0,0' "${DIR_ANALYTICS}/${FILE_GROUPED_MESSAGES_CSV}" "${header_written}")
        remove_file "${queries_temp_file}"
        queries_temp_file=$(mktemp)
      fi

    done < <(tail -n +2 "${DIR_DATA}/${FILE_GROUPS_CSV}" | grep "^${book},")
  done < <(tail -n +2 "${DIR_DATA}/${FILE_PAGES_CSV}")

  if [ -s  "${queries_temp_file}" ]; then
    header_written=$(execute_and_copy_result "${queries_temp_file}" 'null,null,null,null,0,0,0' "${DIR_ANALYTICS}/${FILE_GROUPED_MESSAGES_CSV}" "${header_written}")
    remove_file "${queries_temp_file}"
  fi

  m_end_0=$(date +%s%3N)
  m_elapsed_0=$((m_end_0 - m_start_0))
  echo "INFO: downloaded grouped message in ${m_elapsed_0} ms"
}

download_test_events() {
  echo "INFO: downloading test events ..."
  m_start_0=$(date +%s%3N)

  header_written='false'
  queries_temp_file=$(mktemp)
  while IFS= read -r page_line; do
    book=$(echo "${page_line}" | cut -d',' -f1 | sed 's/[\r\n]//g')
    page=$(echo "${page_line}" | cut -d',' -f7 | sed 's/[\r\n]//g')
    page_start_date=$(echo "${page_line}" | cut -d',' -f2 | sed 's/[\r\n]//g')
    page_start_time=$(echo "${page_line}" | cut -d',' -f3 | sed 's/[\r\n]//g')
    while IFS= read -r scope_line; do
      scope=$(echo "${scope_line}" | cut -d',' -f2 | sed 's/[\r\n]//g')
      echo "SELECT \
          book, page, scope, \
          MAX(rec_date) as max_rec_date, \
          SUM(event_count) as sum_event_count, \
          SUM(z_content_size) as sum_z_content_size, \
          SUM(z_content_uncompressed_size) as sum_z_content_uncompressed_size \
        FROM test_events \
        WHERE book='${book}' AND \
          page='${page}' AND \
          scope='${scope}';" >> "${queries_temp_file}"
      echo "DEBUG: written query for ${book}/${page}/${scope} book/page/scope"

      if [[ $(wc -l < "${queries_temp_file}") -ge "${QUERIES_BATCH_SIZE}" ]]; then
        header_written=$(execute_and_copy_result "${queries_temp_file}" 'null,null,null,null,0,0,0' "${DIR_ANALYTICS}/${FILE_TEST_EVENTS_CSV}" "${header_written}")
        remove_file "${queries_temp_file}"
        queries_temp_file=$(mktemp)
      fi

    done < <(tail -n +2 "${DIR_DATA}/${FILE_SCOPES_CSV}" | grep "^${book},")
  done < <(tail -n +2 "${DIR_DATA}/${FILE_PAGES_CSV}")

  if [ -s  "${queries_temp_file}" ]; then
    header_written=$(execute_and_copy_result "${queries_temp_file}" 'null,null,null,null,0,0,0' "${DIR_ANALYTICS}/${FILE_TEST_EVENTS_CSV}" "${header_written}")
    remove_file "${queries_temp_file}"
  fi

  m_end_0=$(date +%s%3N)
  m_elapsed_0=$((m_end_0 - m_start_0))
  echo "INFO: downloaded test events in ${m_elapsed_0} ms"
}

execute_and_copy_result() {
  local queries_file="${1}"
  local null_line="${2}"
  local output_file="${3}"
  local header_written="${4}"

  local temp_file

  temp_file=$(mktemp)
  command=$(build_cassandra_command_use_file "${queries_file}")
  eval "${command}" | grep "^ " | sed 's/^ *//; s/ *| */,/g' | grep -v "${null_line}" > "${temp_file}"
  exit_code=$?

  if [ "${exit_code}" -eq 0 ]; then
    if [ "${header_written}" = 'false' ]; then
      header_written='true'
      copy_csv_header "${temp_file}" "${output_file}"
    fi
    copy_csv_body "${temp_file}" "${output_file}"
  fi

  remove_file "${temp_file}"
  echo "${header_written}"
}

parse_args "$@"
init

case "${DOWNLOAD_MODE}" in
  "${DOWNLOAD_MODE_BOOKS}")
    download_books
    ;;
  "${DOWNLOAD_MODE_PAGES}")
    download_pages
    ;;
  "${DOWNLOAD_MODE_SESSIONS}")
    download_sessions
    ;;
  "${DOWNLOAD_MODE_GROUPS}")
    download_groups
    ;;
  "${DOWNLOAD_MODE_SCOPES}")
    download_scopes
    ;;
  "${DOWNLOAD_MODE_MESSAGE_STATISTICS}")
    download_message_statistics
    ;;
  "${DOWNLOAD_MODE_MESSAGE_ENTITY_STATISTICS}")
    download_message_entity_statistics
    ;;
  "${DOWNLOAD_MODE_EVENT_ENTITY_STATISTICS}")
    download_event_entity_statistics
    ;;
  "${DOWNLOAD_MODE_GROUPED_MESSAGES}")
    download_grouped_messages
    ;;
  "${DOWNLOAD_MODE_TEST_EVENTS}")
    download_test_events
    ;;
  "${DOWNLOAD_MODE_ALL}")
    download_books
    download_pages
    download_sessions
    download_groups
    download_scopes
    download_message_statistics
    download_message_entity_statistics
    download_event_entity_statistics
    download_grouped_messages
    download_test_events
    ;;
  *)
    echo "CRITICAL: unknown ${ARG_DOWNLOAD_MODE}: ${DOWNLOAD_MODE}"
    download_mode_help
    exit 20
    ;;
esac