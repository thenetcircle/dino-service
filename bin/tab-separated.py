import sys

lines = list()

with open(sys.argv[1], 'r') as f:
    for line in f:
        lines.append(line.replace('\n', '').split(',', maxsplit=3))

with open(f"edited-{sys.argv[1]}", 'w') as f:
    prev_group_id = lines[1][0]
    for line in lines:
        if line[0] != prev_group_id:
            f.write('\t'.join(['', '', '', '']) + '\n')
            prev_group_id = line[0]

        f.write('\t'.join(line) + '\n')
