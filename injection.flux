name: "injection"

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

    - resource: false
      file: "injection-conf.yaml"
      override: true

spouts:
  - id: "filespout"
    className: "org.apache.stormcrawler.spout.FileSpout"
    parallelism: 1
    constructorArgs:
      - "/mnt"
      - "hostnames.gz"
      - true

bolts:
  - id: "generator"
    className: "org.greenwebfoundation.carbontxt.bolt.SeedGenerator"
    parallelism: 1

  - id: "queues"
    className: "org.apache.stormcrawler.opensearch.persistence.QueueBolt"
    parallelism: 2

streams:
  - from: "filespout"
    to: "queues"
    grouping:
      type: FIELDS
      args: ["url"]
      streamId: "status"
