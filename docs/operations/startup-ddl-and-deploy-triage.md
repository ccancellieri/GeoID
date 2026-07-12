# Startup DDL peer races & deploy-failure triage

Foundational modules apply idempotent DDL (`CREATE ... IF NOT EXISTS` / `CREATE OR
REPLACE`) at process startup, guarded by a per-statement advisory lock. When several
instances boot at once — a rolling deploy, a shared-cause restart across pods — two
peers can race the same DDL. The executor tolerates this by design; this runbook
explains what the recovery looks like in the logs and how to triage a startup failure
that a deploy surfaces.

## 1. Reading a recovered peer race

A verified recovery — the peer's object is confirmed to exist by a re-check, not just
assumed — emits a single structured `INFO` line with a stable event name so it is
alertable independently of the surrounding traceback noise:

```
ddl_peer_race_recovered pgcode=<code> service=<name> sync=<bool>
```

If the re-check itself fails (so the original error has to surface), the distinct
event name makes that visible too:

```
ddl_peer_race_recovery_failed service=<name> err=<type>: <message>
```

The startup-DDL lock-timeout / statement-timeout tolerance wrapper (used by
foundational modules that guard their DDL with `run_startup_ddl_tolerating_lock_timeout`)
uses the same pattern across three stages — detection, confirmed recovery, and "still
failing after the unlocked replay":

```
ddl_startup_peer_race_detected   lock_key=<key> reason=<lock_timeout|statement_timeout> service=<name>
ddl_startup_peer_race_recovered  lock_key=<key> reason=<...> service=<name>
ddl_startup_peer_race_unresolved lock_key=<key> reason=<...> retry_reason=<...> service=<name>
```

A `_recovered` line with no matching `_unresolved` line for the same `lock_key` in a
deploy window means the race self-healed cleanly. An `_unresolved` line means the
guarded DDL is still being contested — worth a look, but not necessarily a page: the
statement is idempotent and the next boot (or the next scheduled retry, if the caller
has one) will settle it.

### Querying the recovery rate

Any log query engine that can match on line text works. With `gcloud logging read`
against your service's log sink:

```bash
gcloud logging read '
  resource.type="cloud_run_revision"
  resource.labels.service_name="<your-service>"
  textPayload:"ddl_peer_race_recovered" OR textPayload:"ddl_startup_peer_race_recovered"
' --freshness=1d --format='value(timestamp, textPayload)'
```

A log-based metric on the same filter (count over time) turns "recovery rate spiked"
into an alert without needing to grep tracebacks after the fact.

## 2. Finding the first traceback on a failed deploy

During a rolling deploy, a revision that fails to become healthy can crash-loop in the
same log stream as the new revision that is still booting — the newest lines in the
console are often a *later* crash-loop iteration of the *old* revision, not the actual
error the new revision hit. Automating "the one true root-cause line" out of a mixed,
multi-revision log stream is out of scope here; the manual step below is fast enough
that it has not been worth building tooling for.

**Manual step — per revision, oldest first:**

1. Identify the revision name(s) involved (the one that failed to serve, and the
   fallback still receiving traffic).
2. Query logs scoped to *that revision label specifically*, filtered to `ERROR` and
   above, sorted **ascending** by timestamp — the first hit is the root cause; anything
   after it in the same revision is usually a repeat of the same crash-loop.

```bash
gcloud logging read '
  resource.type="cloud_run_revision"
  resource.labels.service_name="<your-service>"
  resource.labels.revision_name="<the-failing-revision>"
  severity>=ERROR
' --freshness=1d --order=asc --limit=5 --format='value(timestamp, textPayload)'
```

3. Look for `CRITICAL: Foundational module '<Name>' failed during startup` (raised by
   the module lifecycle runner right after it logs the module's own exception with a
   full traceback) or uvicorn's own `Application startup failed` line — whichever comes
   first in that revision's ascending-time slice is the one to read end to end.
4. If the traceback matches one of the tolerated startup-DDL races above (`XX000: tuple
   concurrently updated`, `55P03` lock timeout, `57014` statement timeout) but did
   *not* get a `ddl_*_recovered` line, the existence check could not verify the object
   after the failure — that is a real bug (the DDL never actually completed), not a
   race to tolerate. Anything else is a genuine startup failure unrelated to DDL
   coordination — triage from the traceback itself.
