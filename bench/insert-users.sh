#!/bin/bash
# Run from project root regardless of CWD so `./shard-db` and `db.env` resolve.
cd "$(dirname "$0")/.."
# Insert N auto-generated user records into the 'users' object.
# Usage: ./insert-users.sh [count]
#   count defaults to 1000

COUNT=${1:-1000}
BIN="./shard-db"

echo "Generating $COUNT user records..."

python3 -c "
import json, hashlib, random

count = $COUNT
first_names = ['Alice','Bob','Carol','Dave','Eve','Frank','Grace','Hank','Iris','Jack',
               'Kate','Leo','Mia','Nick','Olga','Paul','Quinn','Rose','Sam','Tina',
               'Uma','Vic','Wendy','Xena','Yuri','Zara','Omar','Layla','Hassan','Fatima']
last_names = ['Smith','Jones','Brown','Wilson','Taylor','Clark','Hall','Lewis','Young','King',
              'Wright','Adams','Baker','Green','Hill','Moore','White','Allen','Scott','Davis',
              'Evans','Thomas','Roberts','Walker','Lee','Khan','Ali','Ahmed','Chen','Park']
domains = ['gmail.com','yahoo.com','outlook.com','company.org','dev.io','mail.co','example.com']
bios = [
    'Software engineer with a passion for open source',
    'Data scientist exploring ML frontiers',
    'Full-stack developer and coffee enthusiast',
    'DevOps engineer automating everything',
    'Product manager building user-centric tools',
    'Designer who codes on the side',
    'Student learning distributed systems',
    'Freelancer specializing in backend APIs',
    'Tech lead at a growing startup',
    'Retired professor turned hobbyist coder',
    'Cloud architect with AWS and GCP experience',
    'Mobile developer building cross-platform apps',
    'Security researcher and CTF player',
    'Database engineer optimizing queries',
    'Frontend developer obsessed with performance',
]

records = []
for i in range(count):
    fn = first_names[i % len(first_names)]
    ln = last_names[i % len(last_names)]
    username = f'{fn.lower()}.{ln.lower()}{i}'
    email = f'{fn.lower()}{i}@{domains[i % len(domains)]}'
    bio = bios[i % len(bios)]
    age = 18 + (i % 60)
    user_id = 100000 + i
    rank = (i % 100) + 1
    score = round(random.uniform(0, 100), 4)
    active = i % 7 != 0  # ~85% active
    level = i % 256
    year = 1960 + (i % 46)
    month = (i % 12) + 1
    day = (i % 28) + 1
    birthday = f'{year}{month:02d}{day:02d}'
    c_month = (i % 12) + 1
    c_day = (i % 28) + 1
    c_hour = i % 24
    c_min = i % 60
    c_sec = (i * 7) % 60
    created_at = f'2024{c_month:02d}{c_day:02d}{c_hour:02d}{c_min:02d}{c_sec:02d}'
    balance = round(random.uniform(-500, 50000), 2)
    hourly_rate = round(random.uniform(10, 500), 4)

    key = hashlib.sha256(f'user-{i}'.encode()).hexdigest()[:32]
    records.append({
        'key': key,
        'value': {
            'username': username,
            'email': email,
            'bio': bio,
            'age': age,
            'user_id': user_id,
            'rank': rank,
            'score': score,
            'active': active,
            'level': level,
            'birthday': birthday,
            'created_at': created_at,
            'balance': str(balance),
            'hourly_rate': str(hourly_rate),
        }
    })

with open('/tmp/shard-db_users.json', 'w') as f:
    json.dump(records, f)

print(f'Generated {count} records -> /tmp/shard-db_users.json')
"

echo "Inserting..."
time $BIN query "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"users\",\"file\":\"/tmp/shard-db_users.json\"}"
echo ""

echo "Verifying..."
$BIN query '{"mode":"size","dir":"default","object":"users"}'
