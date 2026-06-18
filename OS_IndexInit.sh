#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

OSHOST=${1:-"http://localhost:9200"}
OSCREDENTIALS=${2:-"-u opensearch:passwordhere"}

INDICES=("status" "content" "queues")

for index in "${INDICES[@]}"; do
  echo "Deleting and recreating index: $index"

  curl $OSCREDENTIALS -s -XDELETE "$OSHOST/$index/" > /dev/null
  echo "Deleted '$index' index, now recreating it..."
  curl $OSCREDENTIALS -s -XPUT "$OSHOST/$index" -H 'Content-Type: application/json' --upload-file "src/main/resources/$index.mapping"
  echo ""
done

curl $OSCREDENTIALS -s -XDELETE "$OSHOST/metrics*/" >  /dev/null

echo "Deleted 'metrics' index, now recreating it..."

# http://localhost:9200/metrics/_mapping/status?pretty
curl $OSCREDENTIALS -s -XPOST "$OSHOST/_template/metrics-template" -H 'Content-Type: application/json' --upload-file src/main/resources/metrics.mapping

echo ""
