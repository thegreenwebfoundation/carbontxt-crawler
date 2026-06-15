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

spouts:
  - id: "filespout"
    className: "org.apache.stormcrawler.spout.FileSpout"
    parallelism: 1
    constructorArgs:
      - "."
      - "hostnames.gz"
      - false

bolts:
  - id: "generator"
    className: "org.greenwebfoundation.carbontxt.bolt.SeedGenerator"
    parallelism: 1

  - id: "status"
    className: "org.apache.stormcrawler.opensearch.persistence.StatusUpdaterBolt"
    parallelism: 2

streams:
  - from: "filespout"
    to: "generator"
    grouping:
      type: FIELDS
      args: ["url"]

  - from: "generator"
    to: "status"
    grouping:
      streamId: "status"
      type: CUSTOM
      customClass:
        className: "org.apache.stormcrawler.util.URLStreamGrouping"
        constructorArgs:
          - "byDomain"
