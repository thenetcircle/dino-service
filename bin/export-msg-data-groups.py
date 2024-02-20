import sys
import subprocess
import json
import re
from tqdm import tqdm
import traceback
import os


def users_to_group_id(user_a: int, user_b: int) -> str:
    # convert integer ids to hex; need to be sorted
    users = map(hex, sorted([user_a, user_b]))

    # drop the initial '0x' and left-pad with zeros (a uuid is two
    # 16 character parts, so pad to length 16)
    u = "".join([user[2:].zfill(16) for user in users])

    # insert dashes at the correct places
    return f"{u[:8]}-{u[8:12]}-{u[12:16]}-{u[16:20]}-{u[20:]}"


def group_id_to_users(group_id: str) -> (str, str):
    group_id = group_id.replace("-", "")
    user_a = int(group_id[:16].lstrip("0"), 16)
    user_b = int(group_id[16:].lstrip("0"), 16)
    return str(user_a), str(user_b)


def query_db(group_id_):
    res = subprocess.run([
        BIN,
        *OPTIONS,
        QUERY.format(group_id_)
    ], stdout=subprocess.PIPE)

    return str(res.stdout, 'utf-8')


def other_user(group_id_, sender_id_):
    if not group_id_.startswith('00000'):
        return group_id_

    user_a, user_b = group_id_to_users(group_id_)

    if user_a == sender_id_:
        return user_b
    return user_a


GROUP_IDS = sys.argv[1]
OUTPUT_FILE = sys.argv[2]
BIN = os.environ.get("BIN", "cqlsh")
OPTIONS = ["--request-timeout=3600", "casd1", "-k", "dinoms_popp_prod", "--execute"]
QUERY = "select group_id, created_at, user_id, message_payload from messages where group_id = {} order by created_at asc;"

with open(GROUP_IDS, 'r') as f:
    group_ids = f.readlines()
    group_ids = [group_id.replace('\n', '') for group_id in group_ids]

with open(OUTPUT_FILE, 'w') as f:
    f.write('group_id,created_at,sender_id,receiver_id,message_content\n')

for group_id in tqdm(group_ids):
    all_lines = query_db(group_id)

    # split on new line after ending curly brace for content
    outputs = [
        f"{line}}}" for line in all_lines.split('}\n ')
    ]

    # remove the header
    outputs[0] = '0' + outputs[0].split('-\n 0')[1]

    # remove the row count at the end
    outputs[-1] = re.sub('\n\n\(\d+ row(s?)\)\n', '', outputs[-1])
    outputs[-1] = re.sub('}}$', '}', outputs[-1])

    # remove the pipe separation
    split_lines = [output.split(' | ', maxsplit=3) for output in outputs]
    split_lines_parsed_content = list()

    for split_line in split_lines:
        try:
            split_line[-1] = split_line[-1].replace('\\\\"', "'")
            parsed = json.loads(split_line[-1])
            if 'content' not in parsed:
                content = parsed
            else:
                content = parsed['content']

            sender_id = split_line[2]
            receiver_id = other_user(group_id, sender_id)
            split_line = [
                split_line[0],
                split_line[1],
                split_line[2],
                receiver_id,
                json.dumps(content)
            ]

            split_lines_parsed_content.append(split_line)
        except Exception as e:
            print(e)
            traceback.print_exc()
            print(split_line)
            sys.exit(1)

    with open(OUTPUT_FILE, 'a') as f:
        for line in split_lines_parsed_content:
            f.write(f"{','.join(line)}\n")
