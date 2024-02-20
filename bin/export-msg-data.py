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


def query_db(user_id):
    group_id = users_to_group_id(USER_ID_TO_CHECK, int(float(user_id)))

    res = subprocess.run([
        BIN,
        *OPTIONS,
        QUERY.format(group_id)
    ], stdout=subprocess.PIPE)

    return str(res.stdout, 'utf-8')


USER_ID_TO_CHECK = int(float(sys.argv[1]))
OTHER_USER_IDS = sys.argv[2]
OUTPUT_FILE = sys.argv[3]
BIN = os.environ.get("BIN", "cqlsh")
OPTIONS = ["--request-timeout=3600", "casd1", "-k", "dinoms_popp_prod", "--execute"]
QUERY = "select group_id, created_at, user_id, message_payload from messages where group_id = {} order by created_at asc;"

with open(OTHER_USER_IDS, 'r') as f:
    user_ids = f.readlines()
    user_ids = [user_id.replace('\n', '') for user_id in user_ids]

with open(OUTPUT_FILE, 'w') as f:
    f.write('group_id,created_at,sender_id,message_content\n')

for user_id in tqdm(user_ids):
    all_lines = query_db(user_id)

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

            split_line[-1] = json.dumps(content)
            split_lines_parsed_content.append(split_line)
        except Exception as e:
            print(e)
            traceback.print_exc()
            print(split_line)
            sys.exit(1)

    with open(OUTPUT_FILE, 'a') as f:
        for line in split_lines_parsed_content:
            f.write(f"{','.join(line)}\n")
