# Next release — draft notes

Running scratch list for the next tag. **`/release` Step 1 reads this file** and
folds each entry into the GitHub Release body (mostly Self-Hosting / Upgrade
Notes), then clears it in the same PR that publishes the release.

Add an entry the moment you introduce something an operator or self-hoster would
notice — a new env var, a new log line, a new metric, a changed default, a
behaviour change. A commit subject weeks later will not carry it.

Last release: **v0.121.0** (2026-10-03).

---

## Release to-do (pre-cut gates — see `/release` Step 0.2)

## Self-Hosting / Upgrade Notes (fold into the release body)

- **Database migrations take brief exclusive locks on busy tables.** The
  `2026-12-13-110000/110100/110200` migrations redefine foreign keys on child
  tables of `users`, `devices`, `alerts`, `tickets`, `roles` and others. Each
  swap briefly takes an ACCESS EXCLUSIVE lock on the child **and** the parent
  table, and waits at most 5 s for it (`lock_timeout`). If a long-running
  reader holds one of those tables (a large report, a `pg_dump`/backup window),
  the migration stops, the API fails to boot with a lock-timeout error, and it
  re-applies cleanly on the next restart. Avoid deploying this release during a
  database backup window. No data is rewritten; the new constraints are
  validated afterwards without blocking writes.
- **Deleting a role that an access review covered now returns 409** ("Role is
  referenced by access reviews") instead of a server error, so review evidence
  is kept. Deleting a notification channel keeps the alert delivery history it
  produced (shown without a channel name), and deleting a maintenance window
  removes all of its occurrences.

## Features / API

- **Site location pin.** New `POST /api/v1/orgs/sites/:id/location` pins a site's coordinate (and optional 50-1000 m geofence radius) from the field; it requires `sites:set_location` and deliberately no MFA. `PATCH /orgs/sites/:id` also accepts `latitude`/`longitude`/`geofenceRadiusM` (still MFA-gated; both coordinates or neither). Only the site's coordinate is stored, never a technician position. Audited as `site.location_set`.
- **Time entries carry an org and site without a ticket.** `POST /api/v1/time-entries/start` and `POST /api/v1/time-entries` accept optional `orgId`, `siteId` and `source` (`timer`/`location` on start, `location` on create). A ticket's org always wins; a mismatch is `422 ORG_MISMATCH`, a site outside the org is `422 SITE_ORG_MISMATCH`. `GET /time-entries` returns `siteId` and filters by it.
- **Location suggestions (partner setting, off by default).** Ticketing settings → Time Tracking gains a toggle and a default arrival radius (`timeTracking.locationSuggestions`). New `GET /api/v1/time-entries/location-sites` returns the flag, the default radius, whether the caller may pin sites, and the caller's sites (pinned or not; empty without `sites:read`), capped at 2000 with a `truncated` flag, for the mobile app's arrival prompt.

## Migrations

- Adds nullable location columns to `sites` and `site_id` to `time_entries` (no backfill, no table rewrite).
- New `sites:set_location` permission (pin a site's map location from the field), granted to the Partner Technician, Org Technician and Org Admin system roles. Partner Admin already holds `*:*`; custom roles are unchanged.
