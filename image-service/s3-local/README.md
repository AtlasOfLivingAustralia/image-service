# Local S3 for development

A local, S3-compatible object store for running and debugging image-service without AWS. All
traffic goes through [Toxiproxy](https://github.com/Shopify/toxiproxy), so you can also inject
network faults (latency, dropped connections, half-open sockets, ...) when you need to test S3
error handling.

```
image-service / tests  -->  toxiproxy :8666  -->  versitygw S3 :7070
                            (API :8474)
```

The S3 server is the [Versity S3 Gateway](https://github.com/versity/versitygw) (posix backend, data
in tmpfs, so it is wiped when the container stops). MinIO is not used because it no longer
publishes free Docker images.

| Setting | Value |
|---|---|
| Endpoint (via toxiproxy, use this) | `http://localhost:8666` |
| Endpoint (direct, bypasses faults) | `http://localhost:7070` |
| Credentials | `localdev` / `localdev-secret` |
| Region | `us-east-1` (path-style access) |
| Bucket created at startup | `image-service` |

Commands below run from the repo root (`image-service-project-parent/image-service`) with Java 21.

## Running the app against local S3

```bash
docker compose -f image-service/s3-local/docker-compose.yml up -d                  # S3 + toxiproxy (+ creates the bucket)
docker compose -f image-service/s3-local/docker-compose.yml --profile app up -d    # ... + Elasticsearch on :9200
./gradlew :image-service:bootRun -PlocalS3=true
docker compose -f image-service/s3-local/docker-compose.yml --profile app down     # stop everything
```

- `-PlocalS3=true` adds `s3-local/local-s3.yml` as an extra Spring config location. It defines a
  single config-backed S3 storage location, `localS3`, pointing at the proxied endpoint.
- In IntelliJ, add the VM option
  `-Dspring.config.additional-location=optional:file:image-service/s3-local/local-s3.yml` instead.
- The database still comes from your normal external config
  (`/data/image-service/config/image-service-config.yml`).
- Skip the `app` profile if you already run Elasticsearch from `elastic-dc2.yml`; both use port 9200.
- `local-s3.yml` also has commented-out `aws.s3.*` settings (CRT vs Netty, timeouts, derivative
  upload pool) for trying client configurations.

Inspect stored objects directly (bypassing the proxy):

```bash
AWS_ACCESS_KEY_ID=localdev AWS_SECRET_ACCESS_KEY=localdev-secret \
  aws --endpoint-url http://localhost:7070 s3 ls --recursive s3://image-service
```

## Injecting network faults

`s3-local/toxic.sh` drives the Toxiproxy API while the app (or a test) is running:

```bash
image-service/s3-local/toxic.sh latency 500    # add latency (ms) both ways
image-service/s3-local/toxic.sh slow 32        # bandwidth limit (KB/s)
image-service/s3-local/toxic.sh blackhole      # half-open: connections accepted, no data forwarded
image-service/s3-local/toxic.sh reset          # TCP RST
image-service/s3-local/toxic.sh cut 65536      # close after 64 KiB of upload
image-service/s3-local/toxic.sh down / up      # drop all connections / restore
image-service/s3-local/toxic.sh clear          # remove all toxics
image-service/s3-local/toxic.sh list
```

## Automated S3 fault-injection tests

`S3UploadFaultSpec` runs the real `S3StorageOperations` upload paths against the same stack and checks
that uploads behave correctly under faults. It was written to reproduce, and now guards against,
the S3 upload hangs ("... was not made within N seconds of outputStream being invoked",
aws-sdk-java-v2#4893).

```bash
./gradlew :image-service:s3FaultTest                                              # CRT client (default)
./gradlew :image-service:s3FaultTest -Pau.org.ala.images.s3.crt.enabled=false     # Netty client
./gradlew :image-service:s3FaultTest --tests '*S3UploadFaultSpec.uploads recover after connection resets'
```

- The task starts its own containers with Testcontainers; no setup is needed. Add
  `-Ps3local.external=true` to use the docker-compose stack instead (same ports).
- It is separate from `test`; the normal `test` task excludes the `s3local` package.
- If an upload hangs, a thread dump is written to `image-service/build/s3-local/`.
- `S3StorageOperations` reads its settings once per JVM, so client settings are chosen per Gradle
  run. Any `-P`/`-D` property starting with `au.org.ala.images.s3.` or `s3local.` is passed into
  the test JVM.
- Defaults are tighter than production so failures show up quickly: `timeouts.call=5` (also the
  derivative-upload subscribe timeout), `timeouts.eviction=3`, `crt.max-concurrency=8`.

| Property | Default | Meaning |
|---|---|---|
| `s3local.hang.seconds` | 90 | How long before an upload counts as hung (and threads are dumped) |
| `s3local.fault.millis` | 10000 | How long a fault stays applied |
| `s3local.abandon.count` | max-concurrency + 4 (CRT) | Number of leaked streams in the abandoned-stream scenario |
| `s3local.slow.seconds` | call timeout + 5 | Duration of the small slow-writer uploads |
| `s3local.large.mb` / `s3local.large.seconds` | 20 / 4 × call timeout | Size and duration of the multipart-sized slow upload |
| `s3local.external` | false | Use the docker-compose stack instead of Testcontainers |

| Scenario | What it checks |
|---|---|
| baseline | 64 concurrent thumbnail/tile uploads plus 1 original upload succeed |
| abandoned upload streams | Streams opened and never written or closed hold client capacity; the client recovers once they are released |
| producer failure mid-write | A writer that fails and aborts its stream leaves no truncated object |
| blackhole / proxy down-up / resets | In-flight uploads fail (never hang) during a fault, and new uploads succeed once it is lifted |
| slow but healthy producer | A small upload written slowly for longer than the call timeout is not timed out |
| slow multipart-sized upload | 20 MB written slowly (each 8 MB part takes longer than the call timeout) is not timed out and is stored in full |
| cache eviction + DefaultCredentialsProvider | Uploads still work after the S3 client caches are cleared and the old clients closed |

### Debugging

- `./gradlew :image-service:s3FaultTest --debug-jvm --tests '...'`, then attach a debugger on port 5005.
- Or run `S3UploadFaultSpec` from IntelliJ against the compose stack with VM options
  `-Ds3local.enabled=true -Ds3local.external=true -Dau.org.ala.images.s3.timeouts.call=5 -Dau.org.ala.images.s3.crt.max-concurrency=8`,
  and use `toxic.sh` by hand while stopped at breakpoints.
- Useful breakpoints: `S3ByteSinkFactory.openStream` (the `outputStream()` call and `close()`),
  `BlockingOutputStreamAsyncRequestBody.waitForSubscriptionIfNeeded`, `S3StorageOperations.storeInternal`.
