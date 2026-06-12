name: "crawler"

includes:
    - resource: true
      file: "/crawler-default.yaml"
      override: false

    - resource: false
      file: "crawler-conf.yaml"
      override: true

    - resource: false
      file: "opensearch-conf.yaml"
      override: true

spouts:
  - id: "spout"
    className: "org.apache.stormcrawler.opensearch.persistence.AggregationSpout"
    parallelism: 10

bolts:
  - id: "partitioner"
    className: "org.apache.stormcrawler.bolt.URLPartitionerBolt"
    parallelism: 1
  - id: "fetcher"
    className: "org.apache.stormcrawler.bolt.FetcherBolt"
    parallelism: 1
  - id: "carbontxt"
    className: "org.greenwebfoundation.carbontxt.bolt.CarbonTxtBolt"
    parallelism: 1
  - id: "index"
    className: "org.apache.stormcrawler.opensearch.bolt.IndexerBolt"
    parallelism: 1
  - id: "status"
    className: "org.apache.stormcrawler.opensearch.persistence.StatusUpdaterBolt"
    parallelism: 1

streams:
  - from: "spout"
    to: "partitioner"
    grouping:
      type: SHUFFLE

  - from: "partitioner"
    to: "fetcher"
    grouping:
      type: FIELDS
      args: ["key"]

  - from: "fetcher"
    to: "carbontxt"
    grouping:
      type: LOCAL_OR_SHUFFLE

  - from: "carbontxt"
    to: "status"
    grouping:
      type: FIELDS
      args: ["url"]
      streamId: "status"

  - from: "carbontxt"
    to: "index"
    grouping:
      type: LOCAL_OR_SHUFFLE

  - from: "fetcher"
    to: "status"
    grouping:
      type: FIELDS
      args: ["url"]
      streamId: "status"

  - from: "index"
    to: "status"
    grouping:
      type: FIELDS
      args: ["url"]
      streamId: "status"
