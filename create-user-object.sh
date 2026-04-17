#!/bin/bash
# Create the 'users' object with one field of each data type.
# Usage: ./create-user-object.sh

BIN="./shard-db"

$BIN query '{"mode":"create-object","dir":"default","object":"users","splits":16,"max_key":32,"fields":["username:varchar:50","email:varchar:100","bio:varchar:500","age:int","user_id:long","rank:short","score:double","active:bool","level:byte","birthday:date","created_at:datetime","balance:numeric:12,2","hourly_rate:currency"],"indexes":["username","email","age","active","birthday"]}'
