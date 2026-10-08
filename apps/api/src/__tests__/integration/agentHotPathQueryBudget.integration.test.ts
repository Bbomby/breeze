/**
 * #8053 — DB cost budget for the two hottest agent request paths, measured
 * against REAL PostgreSQL.
 *
 * Every agent heartbeats every 60 s and polls `/unifi-collectors` every 30 s,
 * so per-request transaction and statement counts multiply by the whole fleet:
 * at v0.121 the API's single event loop saturated at ~200-250 online agents.
 * These budgets pin the counts after #8053 so a new per-beat transaction or a
 * re-introduced per-request reload shows up as a red test instead of as the
 * next production saturation.
 *
 * How it counts: `postgres` is wrapped so the request pool (the one client
 * named `application_name: 'breeze-api'`, i.e. the production `db`) gets a
 * postgres.js `debug` hook. The hook sees every statement that pool sends —
 * `begin` (a top-level transaction), `savepoint`, the RLS `set_config`
 * prologue and every data statement. Fixture setup uses the separate
 * superuser test client and is never counted. The hook only records while
 * armed, around one request plus a short settle window for work the route
 * defers until after its transaction (`runAfterDbContextExit`).
 *
 * The agent-auth middleware is not mounted here (the agent context is set
 * directly, as in enrollmentReachability.integration.test.ts), so its one
 * transaction per request is outside these numbers; #8053 does not change it.
 *
 * The heartbeat budget is asserted on a STEADY-STATE beat, not on an immediate
 * re-beat: the clock moves 61 s (one beat interval), a sibling device in the
 * same org beats first (so org-wide and global caches are as warm as they are
 * for any org with more than one device), and the device's own Redis policy
 * caches are dropped (their 120 s TTL misses on about every other 60 s beat;
 * this measures the miss). That is the beat production actually pays for.
 *
 * Ratcheted after #8053 W1a-1 (steady 26 / 3 tx, warm 20 / 3, cold 57 / 8) and
 * again after #8142 W03 (steady 14 / 2 tx, warm 14 / 2): one org-scoped
 * post-commit context instead of the OneDrive + shared system contexts, and
 * one policy-set read instead of ten per-feature assignment reads.
 */
import './setup';
import { randomUUID } from 'node:crypto';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { Hono } from 'hono';
import { and, eq, or, sql } from 'drizzle-orm';

const recorder = vi.hoisted(() => ({
  recording: false,
  statements: [] as string[],
  params: [] as unknown[][],
}));

vi.mock('postgres', async (importOriginal) => {
  const actual = await importOriginal<typeof import('postgres')>();
  const original = (actual as unknown as { default: (...args: unknown[]) => unknown }).default;
  const wrapped = (...args: unknown[]) => {
    const urlOrOptions = args[0];
    const options = (typeof urlOrOptions === 'string' ? args[1] : urlOrOptions) as
      | { connection?: { application_name?: string } }
      | undefined;
    if (options?.connection?.application_name === 'breeze-api') {
      const instrumented = {
        ...options,
        debug: (_connection: number, query: string, params?: unknown[]) => {
          if (recorder.recording) {
            recorder.statements.push(query);
            recorder.params.push(params ?? []);
          }
        },
      };
      return typeof urlOrOptions === 'string'
        ? original(urlOrOptions, instrumented)
        : original(instrumented);
    }
    return original(...args);
  };
  Object.assign(wrapped, original);
  return { ...actual, default: wrapped };
});

// Lets a test make a helper read fail with a REAL SQL error inside the
// heartbeat's shared policy transaction (see the savepoint tests):
//  - 'org-flag': the legacy org flag read, the only helper SQL on the policy-set path;
//  - 'builder':  the whole helper builder, which reads its own on the no-set path.
const helperFault = vi.hoisted(() => ({ mode: null as null | 'org-flag' | 'builder' }));
// Forces the policy-set load to fail with a real SQL error (the "set error" path).
const policySetFault = vi.hoisted(() => ({ enabled: false }));
async function divideByZero(): Promise<void> {
  const { db: faultDb } = await import('../../db');
  const { sql: faultSql } = await import('drizzle-orm');
  await faultDb.execute(faultSql`SELECT 1 / 0`); // division_by_zero
}
vi.mock('../../services/helperSettings', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../../services/helperSettings')>();
  return {
    ...actual,
    getOrgHelperSettings: async (...args: Parameters<typeof actual.getOrgHelperSettings>) => {
      if (helperFault.mode === 'org-flag') await divideByZero();
      return actual.getOrgHelperSettings(...args);
    },
    buildHelperConfigUpdate: async (...args: Parameters<typeof actual.buildHelperConfigUpdate>) => {
      if (helperFault.mode === 'builder') await divideByZero();
      return actual.buildHelperConfigUpdate(...args);
    },
  };
});
vi.mock('../../services/devicePolicySet', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../../services/devicePolicySet')>();
  return {
    ...actual,
    loadDevicePolicySet: async (...args: Parameters<typeof actual.loadDevicePolicySet>) => {
      if (policySetFault.enabled) await divideByZero();
      return actual.loadDevicePolicySet(...args);
    },
  };
});

import { db, withDbAccessContext, withSystemDbAccessContext } from '../../db';
import {
  auditLogs,
  configPolicyAssignments,
  configPolicyEventLogSettings,
  configPolicyFeatureLinks,
  configurationPolicies,
  enrollmentKeys,
} from '../../db/schema';
import { createOrganization, createPartner, createSite } from './db-utils';
import { loadDeviceHierarchy } from '../../services/deviceHierarchy';
import { policySetQuery } from '../../services/devicePolicySet';
import {
  buildEventLogConfigUpdate, buildHardwareMonitoringConfigUpdate, buildHelperConfigUpdate,
  buildMonitoringConfigUpdate, buildPamConfigUpdate, buildPatchSourceConfigUpdate,
  buildTimeSyncConfigUpdate, buildWarrantyConfigUpdate, loadOnedriveHelperConfigPlan,
} from '../../routes/agents/helpers';
import { seedPolicy, type SeedLink } from './policySetFixtures';
import { getTestRedis } from './setup';
import { hashEnrollmentKey } from '../../services/enrollmentKeySecurity';
import {
  orgHelperSettingsCache,
  orgPamFallbackCache,
  orgPolicyProbeCache,
} from '../../services/agentOrgSettingsCache';
import { enrollmentRoutes } from '../../routes/agents/enrollment';
import { heartbeatRoutes } from '../../routes/agents/heartbeat';
import { unifiTelemetryRoutes } from '../../routes/agents/unifiTelemetry';

const runDb = it.runIf(!!process.env.DATABASE_URL);

// One heartbeat interval, plus a second so a TTL of exactly 60 s has expired.
const NEXT_BEAT_MS = 61_000;

// #8142 — measured in Task 9 and pinned.
const COLD_TX = 7;
const COLD_STATEMENTS = 47;
const CACHES_MISS_STATEMENTS = 20;
const CONFIGURED_STEADY_STATEMENTS = 21;

// Statement buckets (#8053 W1a-1). Each later lever asserts on its own bucket,
// so a regression names itself instead of showing up as "statements 31 > 26".
// Matched on the whitespace-collapsed, lower-cased SQL postgres.js sends.
const BUCKET_MATCHERS = {
  begin: (s: string) => s === 'begin' || s.startsWith('begin '),
  prologue: (s: string) => s.startsWith("select set_config('breeze.scope'"),
  savepoint: (s: string) => s.startsWith('savepoint'),
  // A resolver's own `select … from devices where id = $1` — NOT the core read
  // (which selects every column, agent_token_hash included).
  deviceLookup: (s: string) =>
    /^select .* from "devices" where "devices"\."id" = \$1/.test(s) && !s.includes('"agent_token_hash"'),
  orgPartnerLookup: (s: string) => /^select "partner_id"(, "type")? from "organizations" where/.test(s),
  groupLookup: (s: string) => s.startsWith('select "group_id" from "device_group_memberships"'),
  hierarchyLoad: (s: string) => s.includes('from "devices" left join "organizations"') && s.includes('jsonb_agg'),
  siteLookup: (s: string) => s.includes('from "devices" inner join "sites"'),
  agentVersions: (s: string) => s.includes('from "agent_versions"'),
  topologyNegotiation: (s: string) => s.includes('from devices where id=$1::uuid and not is_ephemeral'),
  orgHelperSettings: (s: string) => s.startsWith('select "settings" from "organizations"'),
  pamOrgConfig: (s: string) => s.includes('from "pam_org_config"'),
  automationPolicies: (s: string) => s.includes('from "automation_policies"'),
  // #8142: the one-statement policy set, and anything that still reads
  // assignments per feature (must be 0 on every beat shape with a set).
  policySetLoad: (s: string) =>
    s.includes('from "config_policy_assignments"') && s.includes('left join "config_policy_effective_feature_links"'),
  perFeatureAssignmentRead: (s: string) =>
    s.includes('"config_policy_assignments"') && !s.includes('left join "config_policy_effective_feature_links"'),
  // Monitoring's raw-link secondaries (intervals, attachments, replace links) and definitions.
  monitoringSecondary: (s: string) =>
    s.includes('from "config_policy_feature_links"') || s.includes('from "monitor_definitions"'),
  onedriveRead: (s: string) =>
    s.includes('from "config_policy_onedrive_libraries"') || s.includes('from "onedrive_device_state"') || s.includes('from "m365_connections"'),
  peripheralCapabilityWrites: (s: string) =>
    s.startsWith('update "device_commands"') || s.startsWith('update "peripheral_policy_device_states"'),
} as const;

type Bucket = keyof typeof BUCKET_MATCHERS;
const BUCKETS = Object.keys(BUCKET_MATCHERS) as Bucket[];

function normalizeStatement(sql: string): string {
  return sql.trim().toLowerCase().replace(/\s+/g, ' ');
}

function classifyStatement(sql: string): Bucket | null {
  const s = normalizeStatement(sql);
  return BUCKETS.find((bucket) => BUCKET_MATCHERS[bucket](s)) ?? null;
}

interface Measurement {
  status: number;
  transactions: number;
  savepoints: number;
  statements: number;
  buckets: Record<Bucket, number>;
}

function summarize(status: number, statements: string[]): Measurement {
  const buckets = Object.fromEntries(BUCKETS.map((b) => [b, 0])) as Record<Bucket, number>;
  for (const statement of statements) {
    const bucket = classifyStatement(statement);
    if (bucket) buckets[bucket] += 1;
  }
  return {
    status,
    transactions: buckets.begin,
    savepoints: buckets.savepoint,
    statements: statements.length,
    buckets,
  };
}

async function measure(run: () => Promise<Response>): Promise<Measurement> {
  recorder.statements = [];
  recorder.params = [];
  recorder.recording = true;
  try {
    const response = await run();
    // Deferred post-transaction work (runAfterDbContextExit) is part of the
    // per-request cost; give it a moment to land before disarming.
    await new Promise((resolve) => setTimeout(resolve, 250));
    if (process.env.DUMP_8053) {
      console.log(recorder.statements.map((s, i) => `${i}: ${s.replace(/\s+/g, ' ').slice(0, 160)}`).join('\n'));
    }
    return summarize(response.status, recorder.statements);
  } finally {
    recorder.recording = false;
  }
}

// Shifts Date.now() only (cache expiry clocks); real timers and `new Date()`
// are untouched, so the driver and the route behave normally.
let clockOffsetMs = 0;
function advanceClock(ms: number): void {
  clockOffsetMs += ms;
}

async function seedOrg(label: string) {
  const suffix = `${label}-${randomUUID()}`;
  const partner = await createPartner();
  const org = await createOrganization({ partnerId: partner.id });
  const site = await createSite({ orgId: org.id });
  const rawKey = `hotpath-${suffix}`;
  await withSystemDbAccessContext(async () => {
    await db.insert(enrollmentKeys).values({
      orgId: org.id,
      siteId: site.id,
      name: `Hot path ${label}`,
      key: hashEnrollmentKey(rawKey),
      keySecretHash: null,
      usageCount: 0,
      maxUsage: null,
      expiresAt: null,
    });
  });
  return { partnerId: partner.id, orgId: org.id, siteId: site.id, rawKey, suffix };
}

async function seedOrgPolicy(input: {
  orgId: string;
  featureType: 'helper' | 'event_log';
  inlineSettings?: Record<string, unknown>;
  maxEventsPerCycle?: number;
  level: 'device' | 'organization';
  targetId: string;
  roleFilter?: string[];
}): Promise<void> {
  await withSystemDbAccessContext(async () => {
    const [policy] = await db.insert(configurationPolicies).values({
      orgId: input.orgId, partnerId: null, name: `hotpath ${input.featureType} ${randomUUID()}`, status: 'active',
    }).returning();
    const [link] = await db.insert(configPolicyFeatureLinks).values({
      configPolicyId: policy!.id, featureType: input.featureType,
      ...(input.inlineSettings ? { inlineSettings: input.inlineSettings } : {}),
    }).returning();
    if (input.maxEventsPerCycle !== undefined) {
      await db.insert(configPolicyEventLogSettings).values({
        featureLinkId: link!.id, retentionDays: 30, maxEventsPerCycle: input.maxEventsPerCycle,
      });
    }
    await db.insert(configPolicyAssignments).values({
      configPolicyId: policy!.id, level: input.level, targetId: input.targetId, priority: 0,
      ...(input.roleFilter ? { roleFilter: input.roleFilter } : {}),
    });
  });
}

type SeededOrg = Awaited<ReturnType<typeof seedOrg>>;

/**
 * #8053: `/agents/enroll` writes its `agent.enroll` audit row fire-and-forget
 * (createAuditLogAsync), as its own transaction on the instrumented request
 * pool, and returns before that write finishes. If it is still in flight when
 * `measure()` arms the recorder, begin/prologue/insert/commit land inside the
 * measured window and inflate (or flake) the budget. Visibility from another
 * session means the audit transaction has COMMITTED, so polling for the row is
 * deterministic, not a sleep. The recorder is unarmed here, so the polls are
 * never counted. Throws on timeout: never proceed with an unsettled enroll.
 */
async function waitForEnrollAuditCommitted(deviceId: string): Promise<void> {
  for (let attempt = 0; attempt < 1000; attempt++) {
    const rows = await withSystemDbAccessContext(() =>
      db
        .select({ id: auditLogs.id })
        .from(auditLogs)
        .where(and(eq(auditLogs.action, 'agent.enroll'), eq(auditLogs.resourceId, deviceId)))
        .limit(1),
    );
    if (rows.length > 0) return;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error(`agent.enroll audit row for device ${deviceId} never became visible (10s)`);
}

async function enrollDevice(org: SeededOrg, hostname: string) {
  const app = new Hono();
  app.route('/agents', enrollmentRoutes);
  const response = await app.request('/agents/enroll', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      enrollmentKey: org.rawKey,
      hostname: `${hostname}-${org.suffix}`,
      osType: 'linux',
      osVersion: 'Integration Linux',
      architecture: 'amd64',
      agentVersion: '1.0.0-test',
    }),
  });
  expect(response.status).toBe(201);
  const body = await response.json() as { deviceId: string; agentId: string };
  await waitForEnrollAuditCommitted(body.deviceId);
  return {
    deviceId: body.deviceId,
    agentId: body.agentId,
    orgId: org.orgId,
    siteId: org.siteId,
    partnerId: org.partnerId,
  };
}

type EnrolledDevice = Awaited<ReturnType<typeof enrollDevice>>;

function agentApp(device: EnrolledDevice): Hono {
  const app = new Hono();
  app.use('*', async (c, next) => {
    c.set('agent', {
      deviceId: device.deviceId,
      agentId: device.agentId,
      orgId: device.orgId,
      siteId: device.siteId,
      partnerId: device.partnerId,
      role: 'agent',
      isPreAssignment: false,
    } as never);
    await next();
  });
  app.route('/agents', heartbeatRoutes);
  app.route('/agents', unifiTelemetryRoutes);
  return app;
}

// What a current agent declares on every beat: compiledSecurityCapabilities()
// in agent/internal/heartbeat/heartbeat.go (~:7991) plus the runtime PAM
// lifetime version and reconciliation status it sets at ~:4751-4753.
const CURRENT_AGENT_HEARTBEAT = {
  status: 'ok',
  agentVersion: '1.0.0-test',
  metricsAvailable: false,
  securityCapabilities: {
    outboundNetworkPolicyVersion: 1,
    scriptSecretEnvVersion: 1,
    peripheralPolicyProtocolVersion: 2,
    rollbackProtocolVersion: 1,
    revocationLeaseProtocolVersion: 1,
    desktopFenceProtocolVersion: 1,
    desktopWsFenceProtocolVersion: 1,
    consentPromptProtocolVersion: 2,
    pamLifetimeProtocolVersion: 2,
    pamReconciliation: { unresolvedCount: 0, quarantinedCount: 0, awaitingAcknowledgementCount: 0 },
  },
};

// A pre-capability agent (no securityCapabilities). The claim then cancels any
// pending peripheral_policy_sync_v2 rows and marks their states rejected: two
// UPDATEs per beat that a current agent never pays.
const LEGACY_AGENT_HEARTBEAT = { status: 'ok', agentVersion: '1.0.0-test', metricsAvailable: false };

function heartbeat(device: EnrolledDevice, body: Record<string, unknown> = CURRENT_AGENT_HEARTBEAT): Promise<Response> {
  return Promise.resolve(agentApp(device).request(`/agents/${device.agentId}/heartbeat`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  }));
}

function pollCollectors(device: EnrolledDevice): Promise<Response> {
  return Promise.resolve(agentApp(device).request(`/agents/${device.agentId}/unifi-collectors`, { method: 'GET' }));
}

/** Drop the device's own Redis policy caches (helper, pam, event log, ...). */
async function dropDeviceRedisCaches(deviceId: string): Promise<void> {
  const redis = getTestRedis();
  const keys = await redis.keys(`*${deviceId}*`);
  if (keys.length > 0) await redis.del(...keys);
}

describe('agent hot-path DB budget (#8053) — real PostgreSQL', () => {
  beforeEach(() => {
    recorder.recording = false;
    recorder.statements = [];
    clockOffsetMs = 0;
    const realNow = Date.now.bind(Date);
    vi.spyOn(Date, 'now').mockImplementation(() => realNow() + clockOffsetMs);
  });
  afterEach(() => {
    recorder.recording = false;
    vi.restoreAllMocks();
  });

  runDb('the request pool is instrumented (guards the budget tests below against a vacuous zero)', async () => {
    const measured = await measure(async () => {
      await withSystemDbAccessContext(async () => { await db.execute('select 1' as never); });
      return new Response(null, { status: 204 });
    });
    expect(measured.transactions).toBe(1);
    expect(measured.statements).toBeGreaterThanOrEqual(3);
  });

  runDb('POST /agents/:id/heartbeat: a steady-state beat stays inside its transaction and statement budget', async () => {
    const org = await seedOrg('heartbeat');
    const device = await enrollDevice(org, 'target');
    const sibling = await enrollDevice(org, 'sibling');

    const cold = await measure(() => heartbeat(device));
    const warm = await measure(() => heartbeat(device));

    advanceClock(NEXT_BEAT_MS);
    expect((await heartbeat(sibling)).status).toBe(200);
    await dropDeviceRedisCaches(device.deviceId);
    const steady = await measure(() => heartbeat(device));

    console.log(
      '[#8053 budget] heartbeat cold:', JSON.stringify(cold),
      'warm:', JSON.stringify(warm),
      'steady:', JSON.stringify(steady),
    );

    // A current agent declares peripheralPolicyProtocolVersion 2, so the claim
    // never runs the v2 cancel/state UPDATEs that a legacy payload triggers.
    expect(steady.buckets.peripheralCapabilityWrites).toBe(0);

    expect(cold.status).toBe(200);
    expect(warm.status).toBe(200);
    expect(steady.status).toBe(200);
    // #8142 W03 ratchet. Measured after the batched policy read:
    //   steady 14 statements / 2 tx (was 26 / 3), warm 14 / 2 (was 20 / 3),
    //   cold 47 / 7 (was 57 / 8).
    // Pinned at the measured value, not "plus one": a new statement reds this.
    expect(steady.transactions).toBe(2);
    expect(steady.statements).toBeLessThanOrEqual(14);
    expect(warm.transactions).toBe(2);
    expect(warm.statements).toBeLessThanOrEqual(14);
    expect(cold.transactions).toBeLessThanOrEqual(COLD_TX);
    expect(cold.statements).toBeLessThanOrEqual(COLD_STATEMENTS);
    expect(cold.buckets.hierarchyLoad).toBe(1);
    expect(cold.buckets.deviceLookup).toBe(0);
    expect(cold.buckets.topologyNegotiation).toBe(0);
    expect(cold.buckets.agentVersions).toBe(1);
    // Cold has the set, plus exactly ONE other assignment read: the org block's
    // effective-config assignment resolution (cold only; warm and steady skip it
    // via hotPathCache). It is not a per-feature policy read.
    expect(cold.buckets.policySetLoad).toBe(1);
    expect(cold.buckets.perFeatureAssignmentRead).toBe(1);

    // W1a-1 levers stay pulled: one hierarchy read replaces 33 per-resolver
    // reads, on the steady AND the warm beat.
    expect(steady.buckets.hierarchyLoad).toBe(1);
    expect(steady.buckets.deviceLookup).toBe(0);
    expect(warm.buckets.hierarchyLoad).toBe(1);
    expect(warm.buckets.deviceLookup).toBe(0);
    expect(steady.buckets.orgPartnerLookup).toBe(0);
    expect(steady.buckets.groupLookup).toBe(0);
    expect(steady.buckets.siteLookup).toBe(0);
    // Materialization is off for this org, so no topology negotiation runs.
    expect(steady.buckets.topologyNegotiation).toBe(0);
    expect(warm.buckets.topologyNegotiation).toBe(0);
    // The sibling's beat warmed the org's probe, helper-legacy and PAM caches,
    // so this beat reads none of them.
    expect(steady.buckets.automationPolicies).toBe(0);
    expect(steady.buckets.orgHelperSettings).toBe(0);
    expect(steady.buckets.pamOrgConfig).toBe(0);
    expect(steady.buckets.agentVersions).toBe(1);
    expect(warm.buckets.agentVersions).toBe(1);

    // #8142 levers: one policy-set read, no per-feature assignment reads, and a
    // device with no monitors / OneDrive link pays nothing for either.
    for (const beat of [steady, warm]) {
      expect(beat.buckets.policySetLoad).toBe(1);
      expect(beat.buckets.perFeatureAssignmentRead).toBe(0);
      expect(beat.buckets.monitoringSecondary).toBe(0);
      expect(beat.buckets.onedriveRead).toBe(0);
      // The claim's savepoint + the hierarchy/set savepoint. The helper resolves
      // from the set in memory (no savepoint), the probe is a cache hit.
      expect(beat.savepoints).toBe(2);
    }
  });

  runDb('POST /agents/:id/heartbeat: a beat whose per-org caches (probe, helper legacy, PAM fallback) all miss — e.g. a single-device org — still costs 2 transactions; each miss loads in its own savepoint inside the policy context', async () => {
    const org = await seedOrg('permiss');
    const device = await enrollDevice(org, 'target');
    const sibling = await enrollDevice(org, 'sibling');
    expect((await heartbeat(device)).status).toBe(200);
    advanceClock(NEXT_BEAT_MS);
    expect((await heartbeat(sibling)).status).toBe(200);
    await dropDeviceRedisCaches(device.deviceId);
    // Miss ONLY the three per-org caches this wave added; everything else stays warm.
    orgPolicyProbeCache.invalidate(org.orgId);
    orgHelperSettingsCache.invalidate(org.orgId);
    orgPamFallbackCache.invalidate(org.orgId);
    const missed = await measure(() => heartbeat(device));
    console.log('[#8053 budget] per-org caches all miss:', JSON.stringify(missed));

    expect(missed.status).toBe(200);
    expect(missed.transactions).toBe(2);
    expect(missed.buckets.automationPolicies).toBe(1);
    expect(missed.buckets.orgHelperSettings).toBe(1);
    expect(missed.buckets.pamOrgConfig).toBe(1);
    expect(missed.buckets.policySetLoad).toBe(1);
    expect(missed.buckets.perFeatureAssignmentRead).toBe(0);
    expect(missed.statements).toBeLessThanOrEqual(CACHES_MISS_STATEMENTS);
  });

  runDb('POST /agents/:id/heartbeat: a legacy (no securityCapabilities) beat pays the two peripheral-v2 UPDATEs on top', async () => {
    const org = await seedOrg('legacy');
    const device = await enrollDevice(org, 'legacy');
    const sibling = await enrollDevice(org, 'legacy-sibling');
    await heartbeat(device, LEGACY_AGENT_HEARTBEAT);
    advanceClock(NEXT_BEAT_MS);
    expect((await heartbeat(sibling, LEGACY_AGENT_HEARTBEAT)).status).toBe(200);
    await dropDeviceRedisCaches(device.deviceId);
    const steady = await measure(() => heartbeat(device, LEGACY_AGENT_HEARTBEAT));
    console.log('[#8053 budget] legacy steady:', JSON.stringify(steady));

    expect(steady.status).toBe(200);
    expect(steady.transactions).toBe(2);
    expect(steady.buckets.peripheralCapabilityWrites).toBe(2);
    expect(steady.buckets.policySetLoad).toBe(1);
    expect(steady.buckets.perFeatureAssignmentRead).toBe(0);
    expect(steady.statements).toBeLessThanOrEqual(16);
  });

  // Both guards assert the same invariant from two directions: a REAL SQL error
  // inside the shared policy transaction stays inside a savepoint, so every
  // resolver after it still runs in a live transaction. Without the savepoint
  // the division_by_zero aborts the shared transaction, later resolvers fail,
  // and patch_source_settings (always present on a successful resolve, `false`
  // with no policy) would be omitted.
  runDb('a real SQL error in the helper legacy org-flag read (policy-set path) stays inside its savepoint (#8142)', async () => {
    const org = await seedOrg('savepoint-org');
    const device = await enrollDevice(org, 'faulty');
    // The org flag is read only on a per-org cache miss; make sure it misses.
    orgHelperSettingsCache.invalidate(org.orgId);
    await dropDeviceRedisCaches(device.deviceId);
    helperFault.mode = 'org-flag';
    try {
      let body: Record<string, any> = {};
      const measured = await measure(async () => {
        const res = await heartbeat(device);
        body = await res.clone().json() as Record<string, any>;
        return res;
      });
      expect(measured.status).toBe(200);
      // The set loaded (this IS the set path) and the failed statement rolled back to its savepoint...
      expect(measured.buckets.policySetLoad).toBe(1);
      expect(recorder.statements.some((s) => /^rollback to /i.test(s.trim()))).toBe(true);
      // ...so the resolvers after it still delivered.
      expect(body.helperEnabled).toBe(false);
      expect(body.configUpdate?.patch_source_settings).toEqual({ exclusiveWindowsUpdate: false });
    } finally {
      helperFault.mode = null;
    }
  });

  runDb('when the policy-set load fails, the resolvers read their own and a real SQL error in the helper reader stays in its savepoint (#8142)', async () => {
    const org = await seedOrg('savepoint-noset');
    const device = await enrollDevice(org, 'faulty-noset');
    await dropDeviceRedisCaches(device.deviceId);
    policySetFault.enabled = true;
    helperFault.mode = 'builder';
    try {
      let body: Record<string, any> = {};
      const measured = await measure(async () => {
        const res = await heartbeat(device);
        body = await res.clone().json() as Record<string, any>;
        return res;
      });
      expect(measured.status).toBe(200);
      // Two distinct rollbacks: the failed set load and the failed helper read.
      expect(recorder.statements.filter((s) => /^rollback to /i.test(s.trim())).length).toBeGreaterThanOrEqual(2);
      // No set was produced; the resolvers fell back to their own reads.
      expect(measured.buckets.policySetLoad).toBe(0);
      expect(measured.buckets.perFeatureAssignmentRead).toBeGreaterThan(0);
      expect(body.helperEnabled).toBe(false);
      expect(body.configUpdate?.patch_source_settings).toEqual({ exclusiveWindowsUpdate: false });
    } finally {
      policySetFault.enabled = false;
      helperFault.mode = null;
    }
  });

  runDb('GET /agents/:id/unifi-collectors: a device with no collectors costs one probe, then nothing until the TTL', async () => {
    const org = await seedOrg('collectors');
    const device = await enrollDevice(org, 'poller');

    const cold = await measure(() => pollCollectors(device));
    const warm = await measure(() => pollCollectors(device));
    advanceClock(NEXT_BEAT_MS);
    const nextPoll = await measure(() => pollCollectors(device));
    console.log(
      '[#8053 budget] unifi-collectors cold:', JSON.stringify(cold),
      'warm:', JSON.stringify(warm),
      'next minute:', JSON.stringify(nextPoll),
    );

    expect(cold.status).toBe(200);
    expect(warm.status).toBe(200);
    expect(await (await pollCollectors(device)).json()).toEqual({ collectors: [] });
    expect(cold.transactions).toBeLessThanOrEqual(1);
    expect(cold.statements).toBeLessThanOrEqual(4);
    expect(warm.transactions).toBe(0);
    expect(warm.statements).toBe(0);
    expect(nextPoll.statements).toBe(0);
  });

  const CONFIGURED_LINKS = (orgId: string): SeedLink[] => [
    { featureType: 'helper', inlineSettings: { enabled: true } },
    { featureType: 'pam', inlineSettings: { uacInterceptionEnabled: true } },
    { featureType: 'warranty', inlineSettings: { enabled: true, warnDays: 90, criticalDays: 30 } },
    { featureType: 'event_log', maxEventsPerCycle: 250 },
    { featureType: 'hardware_monitoring', pollIntervalMinutes: 15 },
    { featureType: 'patch', exclusiveWindowsUpdate: true },
    { featureType: 'time_sync', ntpServers: ['time.budget.example'] },
    { featureType: 'monitors', serviceName: 'BudgetService', checkIntervalSeconds: 90 },
    { featureType: 'onedrive_helper', orgId, filesOnDemand: true, libraryName: 'Budget Docs' },
  ];

  runDb('POST /agents/:id/heartbeat: a device configured for EVERY feature stays at 2 transactions (#8142)', async () => {
    const org = await seedOrg('configured');
    const device = await enrollDevice(org, 'configured');
    const sibling = await enrollDevice(org, 'configured-sibling');
    await seedPolicy({ owner: { orgId: org.orgId, partnerId: null }, links: CONFIGURED_LINKS(org.orgId),
      assignments: [{ level: 'organization', targetId: org.orgId }] });
    expect((await heartbeat(device)).status).toBe(200);
    advanceClock(NEXT_BEAT_MS);
    expect((await heartbeat(sibling)).status).toBe(200);
    await dropDeviceRedisCaches(device.deviceId);

    let body: Record<string, any> = {};
    const steady = await measure(async () => {
      const res = await heartbeat(device);
      body = await res.clone().json() as Record<string, any>;
      return res;
    });
    console.log('[#8142 budget] configured steady:', JSON.stringify(steady));

    // Non-vacuous: every feature really resolved from the policy.
    expect(body.helperEnabled).toBe(true);
    expect(body.uacInterceptionEnabled).toBe(true);
    expect(body.configUpdate.event_log_settings.max_events_per_cycle).toBe(250);
    expect(body.configUpdate.patch_source_settings).toEqual({ exclusiveWindowsUpdate: true });
    expect(body.configUpdate.monitoring_settings).toMatchObject({ check_interval_seconds: 90, watches: [expect.objectContaining({ name: 'BudgetService' })] });
    expect(body.configUpdate.onedrive_helper_settings.libraries).toHaveLength(1);

    expect(steady.transactions).toBe(2);
    expect(steady.buckets.policySetLoad).toBe(1);
    expect(steady.buckets.perFeatureAssignmentRead).toBe(0);
    expect(steady.statements).toBeLessThanOrEqual(CONFIGURED_STEADY_STATEMENTS);
  });

  runDb('a device whose only policy is PAM pays no monitoring statement and still gets the explicit clear (#8142)', async () => {
    const org = await seedOrg('pamonly');
    const device = await enrollDevice(org, 'pamonly');
    const sibling = await enrollDevice(org, 'pamonly-sibling');
    await seedPolicy({ owner: { orgId: org.orgId, partnerId: null },
      links: [{ featureType: 'pam', inlineSettings: { uacInterceptionEnabled: true } }],
      assignments: [{ level: 'organization', targetId: org.orgId }] });
    expect((await heartbeat(device)).status).toBe(200);
    advanceClock(NEXT_BEAT_MS);
    expect((await heartbeat(sibling)).status).toBe(200);
    await dropDeviceRedisCaches(device.deviceId);

    let body: Record<string, any> = {};
    const steady = await measure(async () => {
      const res = await heartbeat(device);
      body = await res.clone().json() as Record<string, any>;
      return res;
    });
    expect(body.uacInterceptionEnabled).toBe(true);
    expect(body.configUpdate.monitoring_settings).toEqual({ check_interval_seconds: 60, watches: [] });
    expect(steady.buckets.monitoringSecondary).toBe(0);
    expect(steady.transactions).toBe(2);
    expect(steady.statements).toBeLessThanOrEqual(14);
  });

  runDb('cross-tenant assignments forged onto this device never reach its heartbeat (#8142)', async () => {
    const org = await seedOrg('forge');
    const device = await enrollDevice(org, 'forge-target');
    const samePartnerOtherOrg = (await createOrganization({ partnerId: org.partnerId }))!;
    const foreignPartner = (await createPartner())!;
    await seedPolicy({ owner: { orgId: samePartnerOtherOrg.id, partnerId: null },
      links: [{ featureType: 'helper', inlineSettings: { enabled: true, portalUrl: 'https://forged.example' } }],
      assignments: [{ level: 'device', targetId: device.deviceId }], forgeAssignments: true });
    await seedPolicy({ owner: { orgId: null, partnerId: foreignPartner.id },
      links: [{ featureType: 'event_log', maxEventsPerCycle: 999 }],
      assignments: [{ level: 'partner', targetId: org.partnerId, priority: -10 }], forgeAssignments: true });

    // Non-vacuity: the forged rows really exist (system scope sees them), so
    // the assertions below pass because RLS hides them, not because seeding failed.
    const forged = await withSystemDbAccessContext(() =>
      db.select({ policyId: configPolicyAssignments.configPolicyId, level: configPolicyAssignments.level })
        .from(configPolicyAssignments)
        .where(or(
          and(eq(configPolicyAssignments.level, 'device'), eq(configPolicyAssignments.targetId, device.deviceId)),
          and(eq(configPolicyAssignments.level, 'partner'), eq(configPolicyAssignments.targetId, org.partnerId)),
        )));
    expect(forged.map((r) => r.level).sort()).toEqual(['device', 'partner']);

    const res = await heartbeat(device);
    expect(res.status).toBe(200);
    const body = await res.json() as Record<string, any>;
    expect(body.helperEnabled).toBe(false);
    expect(body.helperSettings?.portalUrl).toBeUndefined();
    expect(body.configUpdate.event_log_settings.max_events_per_cycle).toBe(100);
  });

  // Inlines postgres.js parameters as SQL literals so a captured statement can be re-run under EXPLAIN.
  const literal = (v: unknown): string => {
    if (v === null || v === undefined) return 'NULL';
    if (typeof v === 'number' || typeof v === 'boolean') return String(v);
    if (Array.isArray(v)) return `ARRAY[${v.map(literal).join(',')}]::text[]`;
    return `'${String(v).replace(/'/g, "''")}'`;
  };
  const inline = (query: string, params: unknown[]): string =>
    query.replace(/\$(\d+)/g, (_m, n: string) => literal(params[Number(n) - 1]));
  const planText = (rows: unknown): string =>
    (rows as Array<Record<string, string>>).map((r) => r['QUERY PLAN']).join('\n');
  const execMs = (text: string): number => Number(/Execution Time: ([\d.]+) ms/.exec(text)?.[1] ?? NaN);

  it.runIf(!!process.env.DATABASE_URL && !!process.env.EXPLAIN_8142)(
    'perf probe: EXPLAIN (ANALYZE, BUFFERS) of the policy-set statement as breeze_app, configured device (#8142)',
    async () => {
      const org = await seedOrg('explain');
      const device = await enrollDevice(org, 'explain');
      await seedPolicy({ owner: { orgId: org.orgId, partnerId: null }, links: CONFIGURED_LINKS(org.orgId),
        assignments: [{ level: 'organization', targetId: org.orgId }] });
      await withDbAccessContext({
        scope: 'organization', orgId: org.orgId, accessibleOrgIds: [org.orgId], accessiblePartnerIds: [],
        userId: null, currentPartnerId: org.partnerId,
      }, async () => {
        // Evidence that this runs as the unprivileged app role under the org-scoped RLS context.
        const who = (await db.execute(sql`SELECT current_user AS u, current_setting('breeze.scope', true) AS scope,
          current_setting('breeze.org_id', true) AS org, (SELECT rolbypassrls OR rolsuper FROM pg_roles WHERE rolname = current_user) AS bypass`)) as unknown as Array<Record<string, unknown>>;
        console.log('[#8142 probe] role/scope:', JSON.stringify(who[0]));
        expect(who[0]!.u).toBe('breeze_app');
        expect(who[0]!.bypass).toBe(false);
        expect(who[0]!.scope).toBe('organization');

        const hierarchy = (await loadDeviceHierarchy(device.deviceId))!;
        const setPlan = planText(await db.execute(sql`EXPLAIN (ANALYZE, BUFFERS) ${policySetQuery(hierarchy)}`));
        console.log(`[#8142 probe] POLICY-SET STATEMENT PLAN\n${setPlan}`);

        // Legacy: each resolver WITHOUT a policySet issues its own per-feature statement(s).
        recorder.statements = [];
        recorder.params = [];
        recorder.recording = true;
        try {
          await buildHelperConfigUpdate(device.deviceId, org.orgId, { skipCacheRead: true } as never);
          await buildEventLogConfigUpdate(device.deviceId);
          await buildHardwareMonitoringConfigUpdate(device.deviceId);
          await buildPamConfigUpdate(device.deviceId);
          await buildPatchSourceConfigUpdate(device.deviceId);
          await buildWarrantyConfigUpdate(device.deviceId);
          await buildTimeSyncConfigUpdate(device.deviceId);
          await buildMonitoringConfigUpdate(device.deviceId);
          await loadOnedriveHelperConfigPlan(device.deviceId);
        } finally {
          recorder.recording = false;
        }
        const captured = recorder.statements
          .map((q, i) => ({ q, p: recorder.params[i] ?? [] }))
          .filter(({ q }) => /"config_policy_assignments"/i.test(q));
        // Non-vacuity: ten legacy per-feature statements (the policy-set replaces exactly these).
        expect(captured.length).toBe(10);
        let legacyTotal = 0;
        for (const [i, { q, p }] of captured.entries()) {
          const t = planText(await db.execute(sql.raw(`EXPLAIN (ANALYZE, BUFFERS) ${inline(q, p)}`)));
          const ms = execMs(t);
          legacyTotal += ms;
          console.log(`[#8142 probe] LEGACY STATEMENT ${i + 1}/${captured.length} (${ms} ms): ${q.replace(/\s+/g, ' ').slice(0, 110)}...\n${t}`);
        }
        console.log(`[#8142 probe] legacy per-feature assignment statements: ${captured.length}, summed Execution Time ${legacyTotal.toFixed(3)} ms; set statement Execution Time ${execMs(setPlan)} ms`);
      });
    },
  );
});

describe('heartbeat hierarchy pass-through (#8053 W1a-1) — behaviour guards, real PostgreSQL', () => {
  // These pass BEFORE and AFTER the change: they pin today's behaviour so the
  // pass-through cannot alter what an agent receives.

  runDb('a sibling\'s device-level helper policy never reaches another device in the same org', async () => {
    const org = await seedOrg('isolation');
    const device = await enrollDevice(org, 'plain');
    const sibling = await enrollDevice(org, 'helper-on');
    await seedOrgPolicy({
      orgId: org.orgId, featureType: 'helper', inlineSettings: { enabled: true },
      level: 'device', targetId: sibling.deviceId,
    });

    const siblingBody = await (await heartbeat(sibling)).json() as { helperEnabled: boolean };
    const deviceBody = await (await heartbeat(device)).json() as { helperEnabled: boolean };
    expect(siblingBody.helperEnabled).toBe(true);
    expect(deviceBody.helperEnabled).toBe(false);
  });

  runDb('a role this very beat writes is the role the policy resolvers see', async () => {
    const org = await seedOrg('role');
    const device = await enrollDevice(org, 'role');
    await seedOrgPolicy({
      orgId: org.orgId, featureType: 'event_log', maxEventsPerCycle: 777,
      level: 'organization', targetId: org.orgId, roleFilter: ['printer'],
    });

    const res = await heartbeat(device, { ...CURRENT_AGENT_HEARTBEAT, deviceRole: 'printer' });
    expect(res.status).toBe(200);
    const body = await res.json() as { configUpdate: { event_log_settings?: { max_events_per_cycle: number } } };
    expect(body.configUpdate.event_log_settings?.max_events_per_cycle).toBe(777);
  });
});
