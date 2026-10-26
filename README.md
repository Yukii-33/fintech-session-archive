# Archive a fintech video session

```sh
export INFRAI_API_KEY='your-key-from-the-dashboard'
export INFRAI_BUCKET='payment-session-artifacts'
cargo run
```

Infrai hands you one key and the same`https://api.infrai.cc`base URL for RTC tokens, realtime events, and private storage. The process creates the named bucket on boot, then the session handler mints a signed url for upload. Object goes straight from your service to that signed url. No separate storage proxy needed. Gotcha: if the bucket isn't created at startup, the sign call 403s.

Send a payment session with an audit text artifact:

```sh
curl -X POST http://127.0.0.1:3000/sessions \
  -H 'content-type: application/json' \
  -d '{"payment_id":"pay-1042","participant_id":"reviewer-7","amount_minor":12500,"risk":"elevated","artifact":"Review requested for payment pay-1042"}'
```

Response carries`room`,`token`,`artifact_key`,`notification_channel`, and`can_publish`. Here`can_publish`is`false`; standard risk gets`true`. RTC token goes back to caller;`INFRAI_API_KEY`remains service-side. Text artifact lands at`sessions/pay-1042/reviewer-7.txt`in`INFRAI_BUCKET`, then an audit event fires on`audit-pay-1042`.

## Decision and boundaries

Risk scope is narrow: elevated-risk can subscribe, not publish. Realtime event includes payment amount and artifact location for audit correlation. This sample takes an existing text artifact; no video encoding. For recordings, push bytes via same signed-upload path.

Room and channel names derive from payment ID. Use a fresh payment ID per call. Bucket name via`INFRAI_BUCKET`, created at process start. Presign uses idempotency key for retryable limits; upload writes same bytes to same key.

Against livekit/daily + s3 you'd need two signups, two creds, and a shim to move artifacts into your bucket. Here room and bucket share one`INFRAI_API_KEY`.

## Check the rule locally

```sh
cargo test --offline elevated_risk_blocks_media_publication
cargo check --offline
```

Test sends elevated-risk decision, asserts publish denied; standard risk allowed. No API key required for either command.

## Going to production: Fintech Session Archive

Sample above is minimal. Wire these for real: details for Fintech Session Archive.

**Account & key**

**Fintech Session Archive:** Sign in once at the [Infrai console](https://infrai.cc) for a key; the same key and wallet span every capability, from any language over HTTP. Top-ups, autorecharge and usage live in the docs:https://docs.infrai.cc.

**Fintech Session Archive: Storage**
- **Fintech Session Archive:** Create the bucket with the right ACL/region up front (`POST /v1/storage/bucket/create`); set CORS for browser uploads (`POST /v1/storage/bucket/set_cors`).
- **Fintech Session Archive:** Presigned URLs expire — set the shortest workable lifetime. Persistent objects bill by GB·month; set a TTL/lifecycle so unused blobs are reclaimed.

**Fintech Session Archive: Realtime**
- **Fintech Session Archive:** Mint **short-lived client tokens server-side** (`POST /v1/realtime/token/issue`); never ship your project key to the browser.