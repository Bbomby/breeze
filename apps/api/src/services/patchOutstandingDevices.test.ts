import { describe, expect, it } from 'vitest';
import { and, inArray, type SQL } from 'drizzle-orm';
import { PgDialect } from 'drizzle-orm/pg-core';
import { resolveOutstandingDeviceScope } from './patchOutstandingDevices';

const dialect = new PgDialect();
const compile = (conditions: SQL[]) => dialect.sqlToQuery(and(...conditions)!).sql;

const ORG_A = '11111111-1111-4111-8111-111111111111';
const ORG_B = '22222222-2222-4222-8222-222222222222';

/** A stand-in for authMiddleware's orgCondition: an IN-list over the given column. */
const orgConditionFor = (orgIds: string[] | null) => ({
  orgCondition: (column: Parameters<typeof inArray>[0]) => (orgIds === null ? undefined : inArray(column, orgIds)),
});

describe('resolveOutstandingDeviceScope (#9298 follow-up)', () => {
  // device_patches.org_id is restamped with the device on an org move
  // (routes/devices/core.ts getDeviceOrgDenormalizedTables), so constraining
  // it as well as devices.org_id never changes the result. It lets the
  // "All organizations" count use device_patches_org_status_idx (org_id,
  // status) instead of every tenant's pending rows.
  it('scopes device_patches.org_id as well as devices.org_id for a multi-org caller', () => {
    const sql = compile(resolveOutstandingDeviceScope(orgConditionFor([ORG_A, ORG_B]), {})!);
    expect(sql).toContain('"devices"."org_id" in');
    expect(sql).toContain('"device_patches"."org_id" in');
  });

  it('applies ?orgId= to both tables', () => {
    const sql = compile(resolveOutstandingDeviceScope(orgConditionFor([ORG_A, ORG_B]), { orgId: ORG_A })!);
    expect(sql).toContain('"devices"."org_id" =');
    expect(sql).toContain('"device_patches"."org_id" =');
  });

  it('adds no org predicate on either table for an unrestricted (system) caller without ?orgId=', () => {
    const sql = compile(resolveOutstandingDeviceScope(orgConditionFor(null), {})!);
    expect(sql).not.toContain('"devices"."org_id" in');
    expect(sql).not.toContain('"device_patches"."org_id"');
  });

  it('still keeps the device-side exclusions (Quick Support, parked, site restriction)', () => {
    const sql = compile(
      resolveOutstandingDeviceScope(orgConditionFor([ORG_A]), { allowedSiteIds: ['33333333-3333-4333-8333-333333333333'] })!,
    );
    expect(sql).toContain('"devices"."is_ephemeral"');
    expect(sql).toContain("parked_org.type = 'unassigned_pool'");
    expect(sql).toContain('"devices"."site_id" in');
  });

  it('returns null for a site-restricted caller with no sites', () => {
    expect(resolveOutstandingDeviceScope(orgConditionFor([ORG_A]), { allowedSiteIds: [] })).toBeNull();
  });
});
