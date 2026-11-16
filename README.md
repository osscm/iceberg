# iceberg maven-repo (materialized-view prototype)

Static Maven repository hosting an unreleased Apache Iceberg build with the
Materialized View spec core (apache/iceberg#11041 + api/core from apache/iceberg#9830)
applied on top of the `apache-iceberg-1.10.0` release tag.

Version: `org.apache.iceberg:*:1.10.0-mv1`

Resolve via:

```xml
<repository>
  <id>osscm-iceberg-mv</id>
  <url>https://raw.githubusercontent.com/osscm/iceberg/maven-repo/</url>
  <releases><enabled>true</enabled></releases>
  <snapshots><enabled>false</enabled></snapshots>
</repository>
```

Prototype only — not an official Iceberg release; not for production. Will be
retired once the MV spec lands in an upstream Iceberg release.
