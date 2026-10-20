import { compareAgentVersions, parseComparableVersion } from './agentEditionCompat';

/**
 * "Force reboot now" (#8729): an immediate reboot that force-closes running
 * apps. Queued as its own `force_reboot` command type, never as a flag on
 * `reboot`, whose wire shape older agents keep reading. The only door is
 * POST /devices/:id/force-reboot (routes/devices/commands.ts); the exposure
 * contract (__tests__/forceRebootExposure.contract.test.ts) keeps it there.
 */

/**
 * First agent release that knows `force_reboot` (the release after v0.124.0).
 * An older agent answers the type with "unknown command type", so the route
 * refuses such a device before the operator's step-up grant is spent. Called
 * out in the PR for the maintainers to confirm against the release that ships it.
 */
export const MIN_AGENT_VERSION_FORCE_REBOOT = '0.125.0';

/**
 * Core-semver compare, failing CLOSED on anything unparseable, with the same
 * rules as `agentSupportsYaraScan` (services/yaraScanRefs.ts): '' and 'dev'
 * must not compare as "equal to the minimum" (services/systemCleanup.ts), and
 * a prerelease of the minimum (the lab build) counts as the minimum.
 */
export function agentSupportsForceReboot(agentVersion: string | null | undefined): boolean {
  if (typeof agentVersion !== 'string') return false;
  const core = agentVersion.trim().split('-', 1)[0] ?? '';
  if (!parseComparableVersion(core)) return false;
  return compareAgentVersions(core, MIN_AGENT_VERSION_FORCE_REBOOT) >= 0;
}

export type ForceRebootOs = 'windows' | 'macos' | 'linux';

/**
 * The exact argv the agent runs for `force_reboot` on this OS: the mirror of
 * ForceRebootArgv in agent/internal/remote/tools/system.go. Shown to the
 * operator in the confirmation and recorded in the audit entry.
 *
 * Windows adds `/f`: Microsoft documents it as implied only when `/t` > 0, so
 * an immediate reboot without it can stall on "this app is preventing
 * shutdown". `shutdown -r` on macOS and Linux does not wait on applications,
 * so force changes nothing there.
 */
export function forceRebootArgv(osType: ForceRebootOs): string[] | null {
  switch (osType) {
    case 'windows':
      return ['shutdown', '/r', '/f', '/t', '0'];
    case 'macos':
    case 'linux':
      return ['shutdown', '-r', '+0'];
    default:
      return null;
  }
}
