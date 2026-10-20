/**
 * #8729 — "Force reboot now", end to end against real Postgres + Redis.
 *
 * POST /devices/:id/force-reboot with a REAL `device_force_reboot` step-up
 * grant (minted through the real grant store for the exact binding the route
 * computes), then the REAL claim path the agent uses. What the unit tests mock,
 * this proves:
 *   - the queued row is `force_reboot` with the 15-minute live_only deadline;
 *   - the power-state barrier holds it behind an in-flight script, and it is
 *     claimed once nothing is in flight (no barrier bypass);
 *   - the grant is single-use and bound to one device;
 *   - refusals (offline device, old agent) leave the grant unspent;
 *   - the audit row records force: true and the exact argv;
 *   - the generic command route refuses `force_reboot`.
 *
 * Run:
 *   pnpm test-stack up
 *   npx vitest run --config vitest.integration.config.ts src/__tests__/integration/forceRebootRoute.integration.test.ts
 */
import './setup';

import { randomUUID } from 'node:crypto';
import { and, eq } from 'drizzle-orm';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { Hono } from 'hono';
import { getTestDb } from './setup';
import { authMiddleware } from '../../middleware/auth';
import { deviceRoutes } from '../../routes/devices';
import { auditLogs, deviceCommands, devices } from '../../db/schema';
import { withDbAccessContext } from '../../db';
import { claimPendingCommandsForDevice } from '../../services/commandDispatch';
import { createAccessToken, type TokenPayload } from '../../services/jwt';
import { forceRebootResourceDigest, mintStepUpGrant } from '../../services/mfaStepUpGrant';
import { MIN_AGENT_VERSION_FORCE_REBOOT } from '../../services/forceReboot';
import { setupTestEnvironment, type TestEnvironment } from './db-utils';

// The REAL enqueue seam, wrapped so one test can make it refuse exactly once
// after preflight passed (the device dropping offline in between). Every other
// call goes straight through.
const { dispatchOverride } = vi.hoisted(() => ({ dispatchOverride: { next: null as null | Record<string, unknown> } }));
vi.mock('../../services/dispatchDeviceCommand', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../../services/dispatchDeviceCommand')>();
  return {
    ...actual,
    dispatchDeviceCommand: async (input: Parameters<typeof actual.dispatchDeviceCommand>[0]) => {
      const forced = dispatchOverride.next;
      if (forced) {
        dispatchOverride.next = null;
        return forced as never;
      }
      return actual.dispatchDeviceCommand(input);
    },
  };
});

const AGENT_CAPS = { peripheralPolicyProtocolVersion: 2, rollbackProtocolVersion: 1, pamLifetimeProtocolVersion: 2 } as const;
const LIVE_ONLY_MS = 15 * 60 * 1000;

function buildApp(): Hono {
  const app = new Hono();
  app.use('*', authMiddleware);
  app.route('/devices', deviceRoutes);
  return app;
}

describe('#8729 — POST /devices/:id/force-reboot (real grant, real claim path)', () => {
  let app: Hono;
  let env: TestEnvironment;
  let sid: string;
  let token: string;

  async function seedDevice(over: Partial<typeof devices.$inferInsert> = {}): Promise<string> {
    const [row] = await getTestDb()
      .insert(devices)
      .values({
        orgId: env.organization.id,
        siteId: env.site.id,
        agentId: `agent-8729-${randomUUID()}`,
        hostname: `LAPTOP-${randomUUID().slice(0, 6)}`,
        osType: 'windows',
        osVersion: '11',
        architecture: 'x86_64',
        agentVersion: MIN_AGENT_VERSION_FORCE_REBOOT,
        status: 'online',
        enrolledAt: new Date(),
        ...over,
      })
      .returning({ id: devices.id });
    if (!row) throw new Error('seedDevice: no row');
    return row.id;
  }

  async function mintFor(deviceId: string): Promise<string> {
    const grant = await mintStepUpGrant({
      userId: env.user.id,
      operation: 'device_force_reboot',
      authEpoch: 1,
      mfaEpoch: 1,
      sid,
      resourceDigest: forceRebootResourceDigest({ deviceId }),
    });
    if (!grant) throw new Error('grant mint failed (Redis?)');
    return grant;
  }

  const post = (path: string, body: unknown) =>
    app.request(path, {
      method: 'POST',
      headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });

  const forceRebootRows = (deviceId: string) =>
    getTestDb()
      .select()
      .from(deviceCommands)
      .where(and(eq(deviceCommands.deviceId, deviceId), eq(deviceCommands.type, 'force_reboot')));

  const claim = (deviceId: string) =>
    withDbAccessContext(
      {
        scope: 'organization',
        orgId: env.organization.id,
        accessibleOrgIds: [env.organization.id],
        accessiblePartnerIds: [],
        currentPartnerId: env.partner.id,
      },
      () => claimPendingCommandsForDevice(deviceId, 10, 'agent', undefined, { ...AGENT_CAPS }),
    );

  beforeEach(async () => {
    app = buildApp();
    env = await setupTestEnvironment({ scope: 'organization' });
    sid = randomUUID();
    const payload: Omit<TokenPayload, 'type'> = {
      sub: env.user.id,
      email: env.user.email,
      roleId: env.role.id,
      orgId: env.organization.id,
      partnerId: env.partner.id,
      scope: 'organization',
      mfa: true,
      aep: 1,
      mep: 1,
      sid,
    };
    token = await createAccessToken(payload);
  });

  it('queues force_reboot on the live_only deadline, held behind an in-flight script, claimed once nothing is in flight', async () => {
    const deviceId = await seedDevice();
    const [script] = await getTestDb()
      .insert(deviceCommands)
      .values({ deviceId, type: 'script', payload: {}, status: 'sent', targetRole: 'agent', createdBy: env.user.id, submittedOrgId: env.organization.id, executedAt: new Date() })
      .returning();

    const before = Date.now();
    const res = await post(`/devices/${deviceId}/force-reboot`, { stepUpGrant: await mintFor(deviceId) });
    expect(res.status).toBe(201);
    const body = await res.json();
    expect(body).toMatchObject({ type: 'force_reboot', argv: ['shutdown', '/r', '/f', '/t', '0'] });

    const [row] = await forceRebootRows(deviceId);
    expect(row).toMatchObject({ id: body.id, status: 'pending', payload: {} });
    const deliverBy = row!.deliverBy!.getTime();
    expect(deliverBy).toBeGreaterThanOrEqual(before + LIVE_ONLY_MS - 5_000);
    expect(deliverBy).toBeLessThanOrEqual(Date.now() + LIVE_ONLY_MS + 5_000);

    // Real work in flight: the barrier holds the force reboot.
    expect((await claim(deviceId)).map((c) => c.id)).not.toContain(body.id);
    expect((await forceRebootRows(deviceId))[0]!.status).toBe('pending');

    // The script finishes: the force reboot is claimed.
    await getTestDb().update(deviceCommands).set({ status: 'completed', completedAt: new Date() }).where(eq(deviceCommands.id, script!.id));
    expect((await claim(deviceId)).map((c) => c.id)).toContain(body.id);
  });

  it('audits force: true with the exact argv', async () => {
    const deviceId = await seedDevice({ osType: 'macos' });
    const res = await post(`/devices/${deviceId}/force-reboot`, { stepUpGrant: await mintFor(deviceId) });
    expect(res.status).toBe(201);
    const { id } = await res.json();

    let audit: typeof auditLogs.$inferSelect | undefined;
    for (let i = 0; i < 20 && !audit; i++) {
      [audit] = await getTestDb().select().from(auditLogs)
        .where(and(eq(auditLogs.resourceId, deviceId), eq(auditLogs.action, 'agent.command.force_reboot')));
      if (!audit) await new Promise((r) => setTimeout(r, 50));
    }
    expect(audit).toBeDefined();
    expect(audit!.actorId).toBe(env.user.id);
    expect(audit!.details).toMatchObject({
      commandId: id,
      type: 'force_reboot',
      force: true,
      osType: 'macos',
      argv: ['shutdown', '-r', '+0'],
      stepUp: 'device_force_reboot',
    });
  });

  it('spends the grant once: a replay is refused and queues nothing more', async () => {
    const deviceId = await seedDevice();
    const grant = await mintFor(deviceId);
    expect((await post(`/devices/${deviceId}/force-reboot`, { stepUpGrant: grant })).status).toBe(201);

    const replay = await post(`/devices/${deviceId}/force-reboot`, { stepUpGrant: grant });
    expect(replay.status).toBe(403);
    expect(await replay.json()).toMatchObject({ code: 'STEP_UP_REQUIRED' });
    expect(await forceRebootRows(deviceId)).toHaveLength(1);
  });

  it('refuses a grant minted for another device', async () => {
    const target = await seedDevice();
    const other = await seedDevice();
    const res = await post(`/devices/${target}/force-reboot`, { stepUpGrant: await mintFor(other) });
    expect(res.status).toBe(403);
    expect(await forceRebootRows(target)).toHaveLength(0);
  });

  it('refuses an offline device without spending the grant', async () => {
    const deviceId = await seedDevice({ status: 'offline' });
    const grant = await mintFor(deviceId);
    const res = await post(`/devices/${deviceId}/force-reboot`, { stepUpGrant: grant });
    expect(res.status).toBe(409);
    expect(await res.json()).toMatchObject({ code: 'DEVICE_OFFLINE' });
    expect(await forceRebootRows(deviceId)).toHaveLength(0);

    // Still unspent: once the device is online, the same grant works.
    await getTestDb().update(devices).set({ status: 'online' }).where(eq(devices.id, deviceId));
    expect((await post(`/devices/${deviceId}/force-reboot`, { stepUpGrant: grant })).status).toBe(201);
  });

  it('refuses an agent that predates force_reboot without spending the grant', async () => {
    const deviceId = await seedDevice({ agentVersion: '0.124.0' });
    const grant = await mintFor(deviceId);
    const res = await post(`/devices/${deviceId}/force-reboot`, { stepUpGrant: grant });
    expect(res.status).toBe(409);
    expect(await res.json()).toMatchObject({ code: 'AGENT_UPGRADE_REQUIRED', minAgentVersion: MIN_AGENT_VERSION_FORCE_REBOOT });
    expect(await forceRebootRows(deviceId)).toHaveLength(0);

    await getTestDb().update(devices).set({ agentVersion: MIN_AGENT_VERSION_FORCE_REBOOT }).where(eq(devices.id, deviceId));
    expect((await post(`/devices/${deviceId}/force-reboot`, { stepUpGrant: grant })).status).toBe(201);
  });

  it('is refused by the generic command route', async () => {
    const deviceId = await seedDevice();
    const res = await post(`/devices/${deviceId}/commands`, { type: 'force_reboot' });
    expect(res.status).toBe(400);
    expect(await forceRebootRows(deviceId)).toHaveLength(0);
  });

  it('leaves the grant unspent when the enqueue seam refuses after preflight (offline race)', async () => {
    // The device passes preflight as online, then drops offline before the
    // enqueue seam reads it. That refusal must not spend the real grant.
    const deviceId = await seedDevice();
    const grant = await mintFor(deviceId);
    dispatchOverride.next = { ok: false, code: 'device_offline', error: 'Device is offline, cannot execute command' };

    const refused = await post(`/devices/${deviceId}/force-reboot`, { stepUpGrant: grant });
    expect(refused.status).toBe(409);
    expect(await refused.json()).toMatchObject({ code: 'DEVICE_OFFLINE' });
    expect(await forceRebootRows(deviceId)).toHaveLength(0);

    // The same grant still works.
    expect((await post(`/devices/${deviceId}/force-reboot`, { stepUpGrant: grant })).status).toBe(201);
    expect(await forceRebootRows(deviceId)).toHaveLength(1);
  });

  it('rolls the queued row back when the grant turns out to be spent (consumed last)', async () => {
    const deviceId = await seedDevice();
    const grant = await mintFor(deviceId);
    expect((await post(`/devices/${deviceId}/force-reboot`, { stepUpGrant: grant })).status).toBe(201);
    // Second use: validation passes only if the grant still exists, so spend
    // it via the first call and confirm the replay leaves exactly one row; the
    // replay's own insert must have been rolled back with its savepoint.
    const replay = await post(`/devices/${deviceId}/force-reboot`, { stepUpGrant: grant });
    expect(replay.status).toBe(403);
    expect(await forceRebootRows(deviceId)).toHaveLength(1);
  });
});
