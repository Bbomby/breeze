/**
 * "Force reboot now" (#8729): an immediate reboot that force-closes running
 * apps. Queued as its own `force_reboot` command type, never as a flag on
 * `reboot`, whose wire shape older agents keep reading. The only door is
 * POST /devices/:id/force-reboot (routes/devices/forceReboot.ts); the exposure
 * contract (__tests__/forceRebootExposure.contract.test.ts) keeps it there.
 *
 * The minimum agent version, the fail-closed version gate and the per-OS argv
 * live in @breeze/shared so the console's menu gating and confirmation dialog
 * use exactly the same values as this API.
 */
export {
  FORCE_REBOOT_MIN_AGENT_VERSION as MIN_AGENT_VERSION_FORCE_REBOOT,
  agentSupportsForceReboot,
  forceRebootArgv,
  type ForceRebootOs,
} from '@breeze/shared';
