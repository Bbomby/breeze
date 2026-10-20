import { describe, expect, it } from 'vitest';
import {
  MIN_AGENT_VERSION_FORCE_REBOOT,
  agentSupportsForceReboot,
  forceRebootArgv,
} from './forceReboot';
import { forceRebootResourceDigest } from './mfaStepUpGrant';

describe('agentSupportsForceReboot (#8729)', () => {
  it('accepts the minimum version, its prerelease (the lab build), and anything newer', () => {
    expect(agentSupportsForceReboot(MIN_AGENT_VERSION_FORCE_REBOOT)).toBe(true);
    expect(agentSupportsForceReboot(`${MIN_AGENT_VERSION_FORCE_REBOOT}-rc.1`)).toBe(true);
    expect(agentSupportsForceReboot('1.0.0')).toBe(true);
  });

  it('refuses older agents', () => {
    expect(agentSupportsForceReboot('0.124.0')).toBe(false);
    expect(agentSupportsForceReboot('0.99.9')).toBe(false);
  });

  it('fails closed on empty, dev, unparseable or missing versions', () => {
    for (const v of ['', '   ', 'dev', 'unknown', 'v', 'abc.def', null, undefined]) {
      expect(agentSupportsForceReboot(v as string | null | undefined)).toBe(false);
    }
  });
});

describe('forceRebootArgv (#8729)', () => {
  // Must match the agent's ForceRebootArgv (agent/internal/remote/tools/system.go):
  // the operator is shown, and the audit records, exactly what will run.
  it('adds /f on Windows with an immediate reboot', () => {
    expect(forceRebootArgv('windows')).toEqual(['shutdown', '/r', '/f', '/t', '0']);
  });

  it('is the plain immediate reboot on macOS and Linux, where force changes nothing', () => {
    expect(forceRebootArgv('macos')).toEqual(['shutdown', '-r', '+0']);
    expect(forceRebootArgv('linux')).toEqual(['shutdown', '-r', '+0']);
  });

  it('returns null for an OS the agent cannot reboot', () => {
    expect(forceRebootArgv('ios' as never)).toBeNull();
  });
});

describe('forceRebootResourceDigest (#8729)', () => {
  const A = '11111111-1111-4111-8111-111111111111';
  const B = '22222222-2222-4222-8222-222222222222';

  it('is stable for the same device and differs per device', () => {
    expect(forceRebootResourceDigest({ deviceId: A })).toBe(forceRebootResourceDigest({ deviceId: A }));
    expect(forceRebootResourceDigest({ deviceId: A })).not.toBe(forceRebootResourceDigest({ deviceId: B }));
    expect(forceRebootResourceDigest({ deviceId: A })).toMatch(/^sha256:[0-9a-f]{64}$/);
  });
});
