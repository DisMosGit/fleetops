# Scenario coverage ledger

Every capability scenario under `openspec/specs/` and under the active change's delta specs
(`openspec/changes/add-missing-feature-coverage/specs/`) is either backed by a test or exempt with
its reason recorded. An entry names the test function that produces the scenario's trigger and
asserts its outcome, and the file that test lives in, so a reader follows a scenario to its test
without searching. Exemptions are the scenarios this delivery cannot back, in the two categories
named below the entries table.

`go test ./internal/speccoverage/` checks this ledger: it fails when a scenario has neither an
entry nor an exemption, when a row names a scenario the specs do not declare, and when an entry's
test is not declared in the file the entry names.

## Entries

| Capability | Scenario | Test | File |
| --- | --- | --- | --- |
| agent-control-plane-api | One stream carries both directions | TestFleetEndToEnd | internal/agentserver/e2e_test.go |
| agent-control-plane-api | Stream reconnection after loss | TestNoMessageLossAcrossServerKilledStreams | internal/agent/stream_test.go |
| agent-control-plane-api | Heartbeat carries the telemetry fields | TestDeviceRunEmitsOneHeartbeatPerTick | internal/agent/device_test.go |
| agent-control-plane-api | Redelivered heartbeats are identifiable | TestHeartbeatsSignalDeviceWorkflow | internal/agentserver/server_test.go |
| agent-control-plane-api | Command delivery is identifiable | TestCommandDispatchTargetsOneStream | internal/agentserver/server_test.go |
| agent-control-plane-api | Enrolling device registers on the stream | TestRegistrationAccepted | internal/agentserver/server_test.go |
| agent-control-plane-api | Incomplete registration is rejected | TestRegistrationRejectedWritesNothing | internal/agentserver/server_test.go |
| agent-control-plane-api | Firmware is fetched in bounded chunks | TestDownloadFirmwareStream | internal/agentserver/download_test.go |
| agent-control-plane-api | Unknown firmware is refused safely | TestDownloadFirmwareErrors | internal/agentserver/download_test.go |
| agent-control-plane-api | Update progress is reported and acknowledged | TestUpdateStatusAccepted | internal/agentserver/update_test.go |
| agent-control-plane-api | Failure detail travels with the status | TestUpdateHandlerFailures | internal/agent/update_test.go |
| agent-control-plane-api | Duplicate command results are no-ops | TestSignalPathEndToEnd | internal/temporal/signalpath_test.go |
| agent-control-plane-api | Missing idempotency key is rejected | TestReportValidation | internal/agentserver/report_test.go |
| agent-control-plane-api | Errors carry codes, not internals | TestDownloadFirmwareErrors | internal/agentserver/download_test.go |
| agent-emulator | Fleet size comes from configuration | TestFleetIdentities | internal/agent/fleet_test.go |
| agent-emulator | Shutdown stops the whole fleet | TestFleetRunLifecycle | internal/agent/fleet_test.go |
| agent-emulator | Fleet covers rollout target groups | TestFleetIdentities | internal/agent/fleet_test.go |
| agent-emulator | Heartbeat carries the telemetry fields | TestDeviceRunEmitsOneHeartbeatPerTick | internal/agent/device_test.go |
| agent-emulator | Event ids are unique per emission and stable per redelivery | TestDeviceRunEventIDsAreUnique | internal/agent/device_test.go |
| agent-emulator | Degradation lowers health and changes status | TestSimulationStatusPattern | internal/agent/simulate_test.go |
| agent-emulator | Simulation is deterministic under an injected source | TestDeviceRunEmitsOneHeartbeatPerTick | internal/agent/device_test.go |
| agent-firmware-apply | Same seed replays the same outcomes | TestApplierDeterministicOutcomes | internal/agent/apply_test.go |
| agent-firmware-apply | Success rate bounds outcomes | TestApplierRateBounds | internal/agent/apply_test.go |
| agent-firmware-apply | Applied version appears in heartbeats | TestRunAnswersADispatchedCommandThroughTheRelay | cmd/agent/main_test.go |
| agent-firmware-apply | Failed apply keeps the previous version | TestUpdateHandlerFailures | internal/agent/update_test.go |
| agent-firmware-download | Verified download lands on disk | TestFetchVerifiedDownload | internal/agent/download_test.go |
| agent-firmware-download | Failed download leaves no partial file | TestFetchFailures | internal/agent/download_test.go |
| agent-firmware-download | Checksum mismatch fails the download | TestFetchFailures | internal/agent/download_test.go |
| agent-firmware-download | Stream and command checksums must agree | TestFetchFailures | internal/agent/download_test.go |
| agent-firmware-download | Truncated stream fails the download | TestFetchFailures | internal/agent/download_test.go |
| agent-firmware-download | Successful download reports progress and proceeds | TestUpdateHandlerSuccess | internal/agent/update_test.go |
| agent-firmware-download | Failed download reports why and concludes the command | TestUpdateHandlerFailures | internal/agent/update_test.go |
| agent-firmware-download | Retried report reuses the idempotency key | TestUpdateReporterConclude | internal/agent/report_test.go |
| agent-stream-client | Devices register before heartbeating | TestSessionRegistersBeforeHeartbeats | internal/agent/session_test.go |
| agent-stream-client | Reconnect re-registers before resuming | TestSessionRegistersBeforeHeartbeats | internal/agent/session_test.go |
| agent-stream-client | A full queue slows producers instead of growing | TestClientEnqueueBlocksAtBound | internal/agent/client_test.go |
| agent-stream-client | Only sent messages leave the queue | TestWriteLoopKeepsFailedEnvelope | internal/agent/client_test.go |
| agent-stream-client | Backoff grows and is capped | TestReconnectDelaysGrowAndAreCapped | internal/agent/session_test.go |
| agent-stream-client | Success resets the backoff | TestSuccessfulConnectionResetsBackoff | internal/agent/session_test.go |
| agent-stream-client | Reconnect attempts stop at shutdown | TestReconnectDelaysGrowAndAreCapped | internal/agent/session_test.go |
| agent-stream-client | Unsent heartbeats are delivered after reconnect | TestUnsentHeartbeatsSurviveReconnect | internal/agent/session_test.go |
| agent-stream-client | Unanswered requests are retried under their correlation id | TestResendUnansweredKeepsCorrelationID | internal/agent/client_test.go |
| agent-stream-client | Duplicates are always identifiable | TestWriteLoopKeepsFailedEnvelope | internal/agent/client_test.go |
| agent-stream-server | Concurrent agents are served independently | TestCommandDispatchTargetsOneStream | internal/agentserver/server_test.go |
| agent-stream-server | A dropped stream makes its devices unroutable | TestStreamEndMakesDevicesUnroutable | internal/agentserver/server_test.go |
| agent-stream-server | Incomplete registration enrolls nothing | TestRegistrationRejectedWritesNothing | internal/agentserver/server_test.go |
| agent-stream-server | Unenrolled heartbeats are not routed | TestUnenrolledHeartbeatNotRouted | internal/agentserver/server_test.go |
| agent-stream-server | Redelivered heartbeats keep their identity | TestHeartbeatsSignalDeviceWorkflow | internal/agentserver/server_test.go |
| agent-stream-server | A command reaches its target device | TestCommandDispatchTargetsOneStream | internal/agentserver/server_test.go |
| agent-stream-server | Commands never cross agent streams | TestCommandDispatchTargetsOneStream | internal/agentserver/server_test.go |
| agent-stream-server | Dispatch to an unknown device fails visibly | TestCommandDispatchUnknownDevice | internal/agentserver/server_test.go |
| agent-stream-server | A congested session applies backpressure | TestDispatchCongestedSessionTimesOut | internal/agentserver/server_test.go |
| agent-stream-server | Malformed input maps to InvalidArgument | TestMalformedEnvelopeMapsToInvalidArgument | internal/agentserver/server_test.go |
| agent-stream-server | A handler panic is contained | TestServerOptionsInstallsBothInterceptors | internal/agentserver/interceptor_chain_test.go |
| agent-stream-server | Complete report is accepted and forwarded | TestReportForwardsCommandResult | internal/agentserver/report_test.go |
| agent-stream-server | Incomplete report is rejected | TestReportValidation | internal/agentserver/report_test.go |
| agent-stream-server | Routed heartbeats signal the device workflow | TestHeartbeatsSignalDeviceWorkflow | internal/agentserver/server_test.go |
| agent-stream-server | Unenrolled heartbeats signal nothing | TestUnenrolledHeartbeatSignalsNothing | internal/agentserver/server_test.go |
| agent-stream-server | Accepted report is acknowledged and forwarded | TestUpdateStatusAccepted | internal/agentserver/update_test.go |
| agent-stream-server | Malformed report does not kill the stream | TestUpdateStatusRejectedWithoutFailingStream | internal/agentserver/update_test.go |
| broker-observability | Dead-letter depth reflects dead letters | TestSamplerReportsQueueDepths | internal/telemetry/sampler_integration_test.go |
| broker-observability | Work queue backlog is visible | TestSamplerReportsEveryQueueDepth | internal/telemetry/sampler_test.go |
| broker-observability | Retry backlog is visible | TestSamplerReportsEveryQueueDepth | internal/telemetry/sampler_test.go |
| broker-observability | Unreachable broker does not fake a depth | TestSamplerKeepsTheLastMeasuredDepthWhenTheBrokerFails | internal/telemetry/sampler_test.go |
| broker-observability | Lag rises while production outpaces consumption | TestConsumerLagPinsTheDistanceToTheLatestPublishedEvent | internal/telemetry/consumer_test.go |
| broker-observability | Lag returns to zero on catch-up | TestConsumerLagPinsTheDistanceToTheLatestPublishedEvent | internal/telemetry/consumer_test.go |
| broker-observability | Lag never counts backwards | TestConsumerLagPinsTheDistanceToTheLatestPublishedEvent | internal/telemetry/consumer_test.go |
| broker-observability | Lag is reported per consumer | TestRegisterLagIsPerConsumerAndReadAtScrapeTime | internal/telemetry/metrics_test.go |
| broker-observability | Published events are counted by type | TestPublisherPublishesAcceptedHeartbeats | internal/telemetry/publisher_test.go |
| broker-observability | Duplicates are visible as duplicates | TestConsumerAcknowledgesDuplicatesWithoutApplying | internal/telemetry/consumer_test.go |
| broker-observability | Retries and dead letters are counted | TestConsumerRetriesThenDeadLetters | internal/telemetry/consumer_integration_test.go |
| broker-observability | Metrics endpoint exposes the pipeline | TestMetricsHandlerExposesTheEventPipeline | cmd/controlplane/main_test.go |
| broker-observability | Metric names follow the project namespace | TestMetricsExposeThePipeline | internal/telemetry/metrics_test.go |
| device-downgrade | A device that took the firmware is restored | TestDowngradeDeviceActivity | internal/temporal/device_downgrade_test.go |
| device-downgrade | A device that reported a failed restore is reported as failed | TestDowngradeDeviceActivity | internal/temporal/device_downgrade_test.go |
| device-downgrade | A device that never reports runs out of time | TestDowngradeDeviceActivity | internal/temporal/device_downgrade_test.go |
| device-downgrade | A device that never took the firmware is skipped | TestDowngradeDeviceActivity | internal/temporal/device_downgrade_test.go |
| device-downgrade | A device with nothing to restore is reported as unavailable | TestDowngradeDeviceActivity | internal/temporal/device_downgrade_test.go |
| device-downgrade | A restore that cannot be delivered is reported, not raised | TestDowngradeDeviceActivity | internal/temporal/device_downgrade_test.go |
| device-downgrade | A failed read is retried rather than recorded | TestDowngradeDeviceActivity | internal/temporal/device_downgrade_test.go |
| device-downgrade | A retried attempt repeats the command id | TestDowngradeDeviceActivityIsIdempotent | internal/temporal/device_downgrade_test.go |
| device-downgrade | An already restored device is left alone | TestDowngradeDeviceActivityIsIdempotent | internal/temporal/device_downgrade_test.go |
| device-downgrade | A superseded restore is not a success | TestDowngradeDeviceActivity | internal/temporal/device_downgrade_test.go |
| device-liveness | Silent device goes offline | TestStore | internal/devices/store_integration_test.go |
| device-liveness | Reporting device stays online | TestStore | internal/devices/store_integration_test.go |
| device-liveness | Marking is idempotent | TestStore | internal/devices/store_integration_test.go |
| device-liveness | Invalid threshold fails startup | TestValidate | internal/config/validate_test.go |
| device-liveness | Transition is counted once | TestSweepMarksSilentDevicesOfflineAndCounts | cmd/controlplane/main_integration_test.go |
| device-liveness | Steady offline device adds nothing | TestOfflineTransitionsCountedOncePerTransition | cmd/controlplane/main_test.go |
| device-liveness | Metric is scrapeable | TestMetricsHandlerExposesOfflineTransitions | cmd/controlplane/main_test.go |
| device-registry | First connection creates the device record | TestRegistrationAccepted | internal/agentserver/server_test.go |
| device-registry | Reconnection updates the record in place | TestStore | internal/devices/store_integration_test.go |
| device-registry | Rejected registration writes nothing | TestRegistrationRejectedWritesNothing | internal/agentserver/server_test.go |
| device-registry | Accepted heartbeat refreshes last seen | TestWriterIngest | internal/telemetry/writer_integration_test.go |
| device-registry | Registration seeds the last-seen timestamp | TestRegistrationAccepted | internal/agentserver/server_test.go |
| device-registry | Last seen survives status changes | TestStore | internal/devices/store_integration_test.go |
| device-search-attributes | Attribute values mirror device state | TestDeviceWorkflowSearchAttributes | internal/temporal/scheduling_test.go |
| device-search-attributes | Unknown attributes stay empty | TestDeviceWorkflowSearchAttributes | internal/temporal/scheduling_test.go |
| device-search-attributes | Firmware change updates the firmware attribute | TestDeviceWorkflowSearchAttributes | internal/temporal/scheduling_test.go |
| device-search-attributes | Going offline flips the online attribute | TestDeviceWorkflowSearchAttributes | internal/temporal/scheduling_test.go |
| device-search-attributes | Returning to online flips the online attribute back | TestDeviceWorkflowSearchAttributes | internal/temporal/scheduling_test.go |
| device-search-attributes | Filtering works after a rollover | TestDeviceWorkflowSearchAttributes | internal/temporal/scheduling_test.go |
| device-search-attributes | Fresh namespace gets the attributes | TestBootstrapNamespace | cmd/worker/main_test.go |
| device-search-attributes | Re-registration is a no-op | TestEnsureSearchAttributes | internal/temporal/searchattrs_test.go |
| device-state-snapshot | Idle device keeps getting snapshots | TestDeviceWorkflowSnapshotScheduling | internal/temporal/scheduling_test.go |
| device-state-snapshot | Cadence follows configuration | TestDeviceWorkflowSnapshotScheduling | internal/temporal/scheduling_test.go |
| device-state-snapshot | Firmware adoption snapshots immediately | TestDeviceWorkflowSnapshotScheduling | internal/temporal/scheduling_test.go |
| device-state-snapshot | Command lifecycle snapshots immediately | TestDeviceWorkflowSnapshotScheduling | internal/temporal/scheduling_test.go |
| device-state-snapshot | Routine heartbeat defers to the periodic snapshot | TestDeviceWorkflowSnapshotScheduling | internal/temporal/scheduling_test.go |
| device-state-snapshot | Snapshot reflects the complete state | TestSnapshotActivityPayload | internal/temporal/snapshot_test.go |
| device-state-snapshot | Retried write converges | TestSnapshotStore | internal/devices/snapshots_integration_test.go |
| device-state-snapshot | Failed snapshot leaves the entity running | TestDeviceWorkflowSnapshotScheduling | internal/temporal/scheduling_test.go |
| device-update | A device that applies the update reports success | TestUpdateDeviceActivity | internal/temporal/device_update_test.go |
| device-update | A device that fails the update reports the failure | TestUpdateDeviceActivity | internal/temporal/device_update_test.go |
| device-update | A device that never reports runs out of time | TestUpdateDeviceActivity | internal/temporal/device_update_test.go |
| device-update | A result reported before the first observation is found | TestUpdateDeviceActivity | internal/temporal/device_update_test.go |
| device-update | A superseded command is never a success | TestUpdateDeviceActivity | internal/temporal/device_update_test.go |
| device-update | An undeliverable command fails the activity | TestUpdateDeviceActivity | internal/temporal/device_update_test.go |
| device-workflow | First signal brings the workflow into existence | TestSignalPathEndToEnd | internal/temporal/signalpath_test.go |
| device-workflow | State query returns the complete picture | TestDeviceWorkflowStateQuery | internal/temporal/device_workflow_test.go |
| device-workflow | A device that never changed firmware records no previous version | TestDeviceWorkflowStateQuery | internal/temporal/device_workflow_test.go |
| device-workflow | The version being left becomes the previous one | TestDeviceStateApply | internal/temporal/device_state_test.go |
| device-workflow | A report that changes nothing moves nothing | TestDeviceStateApply | internal/temporal/device_state_test.go |
| device-workflow | The previous version survives a continuation | TestDeviceWorkflowPreviousFirmware | internal/temporal/device_workflow_test.go |
| device-workflow | Heartbeat refreshes liveness | TestDeviceStateApply | internal/temporal/device_state_test.go |
| device-workflow | Heartbeat reports a firmware change | TestDeviceStateApply | internal/temporal/device_state_test.go |
| device-workflow | Stale heartbeat changes nothing | TestDeviceStateApply | internal/temporal/device_state_test.go |
| device-workflow | Issued command becomes pending and is delivered | TestDeviceWorkflowCommandLifecycle | internal/temporal/device_workflow_test.go |
| device-workflow | Newer command supersedes the pending one | TestDeviceWorkflowCommandLifecycle | internal/temporal/device_workflow_test.go |
| device-workflow | Repeated issuance of the same command is a no-op | TestDeviceWorkflowSignalIdempotency | internal/temporal/device_workflow_test.go |
| device-workflow | Matching result clears the pending command | TestDeviceWorkflowCommandLifecycle | internal/temporal/device_workflow_test.go |
| device-workflow | The concluded command is recorded and readable | TestDeviceWorkflowStateQuery | internal/temporal/device_workflow_test.go |
| device-workflow | Success adopts the commanded firmware | TestDeviceWorkflowCommandLifecycle | internal/temporal/device_workflow_test.go |
| device-workflow | A restore moves the previous version too | TestDeviceStateApply | internal/temporal/device_state_test.go |
| device-workflow | Failure leaves firmware unchanged | TestDeviceWorkflowCommandLifecycle | internal/temporal/device_workflow_test.go |
| device-workflow | Result for a non-pending command changes nothing | TestDeviceWorkflowCommandLifecycle | internal/temporal/device_workflow_test.go |
| device-workflow | Newer configuration version is applied | TestDeviceStateApply | internal/temporal/device_state_test.go |
| device-workflow | Older or repeated configuration version is ignored | TestDeviceStateApply | internal/temporal/device_state_test.go |
| device-workflow | Duplicate heartbeat is dropped | TestDeviceWorkflowSignalIdempotency | internal/temporal/device_workflow_test.go |
| device-workflow | Duplicate command result is dropped | TestDeviceStateApply | internal/temporal/device_state_test.go |
| device-workflow | Duplicate signals across a continuation are dropped | TestDeviceWorkflowRollingContinuation | internal/temporal/device_workflow_test.go |
| device-workflow | State survives the rollover unchanged | TestDeviceWorkflowRollingContinuation | internal/temporal/device_workflow_test.go |
| device-workflow | Signals in flight around a rollover are applied exactly once | TestDeviceWorkflowRollingContinuation | internal/temporal/device_workflow_test.go |
| device-workflow | Reported phase lands in device state | TestDeviceWorkflowUpdateStatus | internal/temporal/updatestatus_test.go |
| device-workflow | Latest progress wins | TestApplyUpdateStatus | internal/temporal/updatestatus_test.go |
| device-workflow | Update status does not conclude the command | TestDeviceWorkflowUpdateStatus | internal/temporal/updatestatus_test.go |
| device-workflow | Identity attributes are adopted | TestDeviceStateIdentityAdoption | internal/temporal/device_state_test.go |
| device-workflow | Silence past the threshold flips the status offline | TestRefreshLiveness | internal/temporal/device_state_test.go |
| device-workflow | A new heartbeat flips the status back online | TestRefreshLiveness | internal/temporal/device_state_test.go |
| device-workflow | Status survives the rollover | TestCarryRoundTrip | internal/temporal/device_state_test.go |
| firmware-download-server | Metadata precedes the chunk stream | TestDownloadFirmwareStream | internal/agentserver/download_test.go |
| firmware-download-server | Chunks are bounded and contiguous | TestDownloadFirmwareStream | internal/agentserver/download_test.go |
| firmware-download-server | Unknown firmware is refused safely | TestDownloadFirmwareErrors | internal/agentserver/download_test.go |
| firmware-download-server | Incomplete request is rejected | TestDownloadFirmwareErrors | internal/agentserver/download_test.go |
| firmware-inventory | A record that disagrees with the device is corrected | TestRollbackPlanRunsAndIsRecorded | internal/temporal/rollback_workflow_test.go |
| firmware-inventory | A record that already agrees is left alone | TestReconcileFirmware | internal/devices/reconcile_test.go |
| firmware-inventory | Every device the rollback touched is reconciled | TestRollbackRecordsWhatItCouldNotRestore | internal/temporal/rollback_workflow_test.go |
| firmware-inventory | The reconciliation follows the downgrades | TestRollbackPlanRunsAndIsRecorded | internal/temporal/rollback_workflow_test.go |
| firmware-inventory | A retried reconciliation writes nothing | TestStore | internal/devices/store_integration_test.go |
| firmware-inventory | Only a reported version is written | TestStore | internal/devices/store_integration_test.go |
| firmware-inventory | A device with no record is reported rather than invented | TestReconcileInventoryActivity | internal/temporal/device_inventory_test.go |
| firmware-inventory | A device that holds no version is never used to clear a record | TestReconcileInventoryActivity | internal/temporal/device_inventory_test.go |
| firmware-inventory | The inventory accounts for every device exactly once | TestRollbackBookkeepingMatchesTheDevices | cmd/worker/rollout_restart_rollback_scenarios_integration_test.go |
| firmware-inventory | Corrections are reported alongside the inventory | TestRolloutSmoke | cmd/worker/rollout_integration_test.go |
| firmware-inventory | A restored device is recorded on the version it runs | TestRollbackBookkeepingMatchesTheDevices | cmd/worker/rollout_restart_rollback_scenarios_integration_test.go |
| firmware-inventory | A device the rollback could not restore is recorded on what it still runs | TestRollbackBookkeepingMatchesTheDevices | cmd/worker/rollout_restart_rollback_scenarios_integration_test.go |
| firmware-inventory | The records agree with the devices when a rollback concludes | TestRollbackBookkeepingMatchesTheDevices | cmd/worker/rollout_restart_rollback_scenarios_integration_test.go |
| firmware-registry | Successful upload returns the firmware record | TestUpload | internal/firmware/upload_test.go |
| firmware-registry | Malformed upload is rejected without side effects | TestUpload | internal/firmware/upload_test.go |
| firmware-registry | Recorded checksum matches the stored binary | TestFirmwarePipeline | internal/firmware/pipeline_integration_test.go |
| firmware-registry | Uploads larger than any transfer buffer succeed | TestFirmwarePipeline | internal/firmware/pipeline_integration_test.go |
| firmware-registry | Metadata references the GridFS object | TestStore | internal/firmware/store_integration_test.go |
| firmware-registry | Failed metadata write leaves no orphan | TestStore | internal/firmware/store_integration_test.go |
| firmware-registry | Firmware for known models is accepted | TestUpload | internal/firmware/upload_test.go |
| firmware-registry | Unknown target model is refused | TestUpload | internal/firmware/upload_test.go |
| firmware-registry | Empty model list is refused | TestUpload | internal/firmware/upload_test.go |
| firmware-registry | Re-uploading a version is refused | TestUpload | internal/firmware/upload_test.go |
| firmware-registry | Concurrent duplicate versions collapse to one record | TestUploadConcurrentVersionsCollapseToOneRecord | internal/firmware/workflow_registry_test.go |
| firmware-registry | A claimed version resolves to its record | TestMetadataByVersion | internal/firmware/store_test.go |
| firmware-registry | An unclaimed version is not found | TestMetadataByVersion | internal/firmware/store_test.go |
| health-readiness | Process is alive | TestLiveness | internal/health/health_test.go |
| health-readiness | Liveness stays green while dependencies are down | TestLiveness | internal/health/health_test.go |
| health-readiness | All dependencies reachable | TestReadinessAllGreen | internal/health/health_test.go |
| health-readiness | One dependency down | TestReadinessOneDown | internal/health/health_test.go |
| health-readiness | Readiness recovers without restart | TestReadinessRecoversWithoutRestart | internal/health/health_test.go |
| health-readiness | Readiness answers promptly when everything is down | TestReadinessBoundedWhenAllDown | internal/health/health_test.go |
| health-readiness | Error summaries never leak credentials | TestReadinessSummariesNeverLeakCredentials | internal/health/health_test.go |
| heartbeat-consumption | First delivery is processed once | TestConsumerProcessesAClaimedEvent | internal/telemetry/consumer_test.go |
| heartbeat-consumption | Redelivery is a counted no-op | TestConsumerAcknowledgesDuplicatesWithoutApplying | internal/telemetry/consumer_test.go |
| heartbeat-consumption | Interrupted work is resumed, not skipped | TestConsumerResumesAnUnfinishedClaim | internal/telemetry/consumer_test.go |
| heartbeat-consumption | A second record for one pair is refused | TestLedgerClaimAndComplete | internal/telemetry/dedup_integration_test.go |
| heartbeat-consumption | Failed side effect is not acknowledged | TestConsumerRetriesAFailedDeliveryIntoTheAttemptsQueue | internal/telemetry/consumer_test.go |
| heartbeat-consumption | Degraded heartbeat opens an alert | TestAlertingAgainstStore | internal/telemetry/alerting_integration_test.go |
| heartbeat-consumption | Repeated degradation refreshes one alert | TestAlertingAgainstStore | internal/telemetry/alerting_integration_test.go |
| heartbeat-consumption | Healthy heartbeat writes nothing | TestAlertingThreshold | internal/telemetry/alerting_test.go |
| heartbeat-consumption | Alert write is idempotent | TestAlertingAgainstStore | internal/telemetry/alerting_integration_test.go |
| heartbeat-consumption | Failure enters the retry ladder | TestConsumerRetriesAFailedDeliveryIntoTheAttemptsQueue | internal/telemetry/consumer_test.go |
| heartbeat-consumption | Exhausted attempts dead-letter with a reason | TestConsumerDeadLettersAnExhaustedDelivery | internal/telemetry/consumer_test.go |
| heartbeat-consumption | Unrepublishable failure cannot loop | TestConsumerRejectsWhenTheRetryRepublishIsRefused | internal/telemetry/consumer_test.go |
| heartbeat-consumption | Malformed payload goes straight to the dead-letter queue | TestConsumerDeadLettersMalformedPayloadsWithoutRetrying | internal/telemetry/consumer_test.go |
| heartbeat-consumption | Unsupported schema version is refused | TestConsumerDeadLettersMalformedPayloadsWithoutRetrying | internal/telemetry/consumer_test.go |
| heartbeat-consumption | Event missing its identifier is refused | TestConsumerDeadLettersMalformedPayloadsWithoutRetrying | internal/telemetry/consumer_test.go |
| heartbeat-event-publication | One accepted heartbeat publishes one event | TestPublisherAgainstBroker | internal/telemetry/publisher_integration_test.go |
| heartbeat-event-publication | Publication follows ingest acceptance | TestFanoutNeverPublishesARefusedHeartbeat | internal/telemetry/fanout_test.go |
| heartbeat-event-publication | Envelope carries identity, timing, and samples | TestPublisherAgainstBroker | internal/telemetry/publisher_integration_test.go |
| heartbeat-event-publication | Sequence orders events within a publisher | TestPublisherPublishesAcceptedHeartbeats | internal/telemetry/publisher_test.go |
| heartbeat-event-publication | Message properties identify the event | TestEnvelopeMessageProperties | internal/telemetry/envelope_test.go |
| heartbeat-event-publication | Unreachable broker degrades the fan-out only | TestEventPipelineEndToEnd | cmd/controlplane/pipeline_integration_test.go |
| heartbeat-event-publication | Full publish buffer sheds load instead of stalling streams | TestPublisherShedsWhenBufferFull | internal/telemetry/publisher_test.go |
| heartbeat-event-publication | Publication resumes after the broker returns | TestEventPipelineEndToEnd | cmd/controlplane/pipeline_integration_test.go |
| heartbeat-event-publication | Confirmed publish counts as published | TestPublisherPublishesAcceptedHeartbeats | internal/telemetry/publisher_test.go |
| heartbeat-event-publication | Lost connection is retried, not fatal | TestPublisherAgainstBroker | internal/telemetry/publisher_integration_test.go |
| message-broker-topology | Heartbeat event reaches the heartbeat consumer queue | TestTopologyAgainstBroker | internal/telemetry/topology_integration_test.go |
| message-broker-topology | Rollout work reaches the rollout work queue | TestTopologyAgainstBroker | internal/telemetry/topology_integration_test.go |
| message-broker-topology | A notification reaches the notification queue | TestRollbackNotificationsAgainstBroker | internal/telemetry/rollback_notification_integration_test.go |
| message-broker-topology | Narrow binding selects one region | TestTopologyAgainstBroker | internal/telemetry/topology_integration_test.go |
| message-broker-topology | Work queue names are the consumer contract | TestDeclareLayout | internal/telemetry/topology_test.go |
| message-broker-topology | A notification is routable before a consumer exists | TestRollbackNotificationsAgainstBroker | internal/telemetry/rollback_notification_integration_test.go |
| message-broker-topology | First failure waits the base delay | TestConsumerRetriesThenDeadLetters | internal/telemetry/consumer_integration_test.go |
| message-broker-topology | Each further failure waits twice as long | TestRetryDelay | internal/telemetry/topology_test.go |
| message-broker-topology | Retry preserves the event | TestForwardMessage | internal/telemetry/envelope_test.go |
| message-broker-topology | Retry attempts are bounded | TestConsumerRetriesThenDeadLetters | internal/telemetry/consumer_integration_test.go |
| message-broker-topology | Exhausted attempts end in the dead-letter queue | TestConsumerRetriesThenDeadLetters | internal/telemetry/consumer_integration_test.go |
| message-broker-topology | Undeliverable failure does not vanish | TestTopologyAgainstBroker | internal/telemetry/topology_integration_test.go |
| message-broker-topology | Dead letters wait for an operator | TestTopologyAgainstBroker | internal/telemetry/topology_integration_test.go |
| message-broker-topology | Declaration is repeatable | TestTopologyAgainstBroker | internal/telemetry/topology_integration_test.go |
| message-broker-topology | Conflicting declaration fails loudly | TestTopologyDeclarationConflictFailsLoudly | internal/telemetry/topology_integration_test.go |
| mongo-data-model | Device record content | TestStore | internal/devices/store_integration_test.go |
| mongo-data-model | Required fields enforced | TestSchema | internal/mongotest/schema_test.go |
| mongo-data-model | Device state updates in place | TestStore | internal/devices/store_integration_test.go |
| mongo-data-model | A stale recorded version is corrected against the device | TestStore | internal/devices/store_integration_test.go |
| mongo-data-model | A record that already agrees is left alone | TestStore | internal/devices/store_integration_test.go |
| mongo-data-model | Firmware metadata with binary reference | TestStore | internal/firmware/store_integration_test.go |
| mongo-data-model | Rollout record content | TestStore | internal/rollout/store_integration_test.go |
| mongo-data-model | Rollout status transitions are recorded | TestStore | internal/rollout/store_integration_test.go |
| mongo-data-model | A paused rollout is recorded as paused | TestRolloutWorkflowControlSignalsAreIdempotent | internal/temporal/rollout_control_test.go |
| mongo-data-model | A compensating rollout is recorded as rolling back | TestStore | internal/rollout/store_integration_test.go |
| mongo-data-model | A rollout that could not start is recorded as failed | TestRolloutWorkflowCannotStart | internal/temporal/rollout_workflow_test.go |
| mongo-data-model | A terminal status is not overwritten | TestStore | internal/rollout/store_integration_test.go |
| mongo-data-model | A retried rollback write converges | TestStore | internal/rollout/store_integration_test.go |
| mongo-data-model | Wave record content | TestStore | internal/rollout/store_integration_test.go |
| mongo-data-model | Wave membership is the record of who was targeted | TestStore | internal/rollout/store_integration_test.go |
| mongo-data-model | A retried wave write converges | TestStore | internal/rollout/store_integration_test.go |
| mongo-data-model | Redelivered events are no-ops | TestWriterIngest | internal/telemetry/writer_integration_test.go |
| mongo-data-model | Region and model pool queries | TestSchema | internal/mongotest/schema_test.go |
| mongo-data-model | Duplicate refusal is distinguishable | TestSchema | internal/mongotest/schema_test.go |
| mongo-data-model | Snapshot document content | TestSnapshotStore | internal/devices/snapshots_integration_test.go |
| mongo-data-model | Snapshots update in place | TestSnapshotStore | internal/devices/snapshots_integration_test.go |
| mongo-data-model | One ledger document per consumer and event | TestLedgerClaimAndComplete | internal/telemetry/dedup_integration_test.go |
| mongo-data-model | Uniqueness is enforced by the store | TestSchema | internal/mongotest/schema_test.go |
| mongo-data-model | Ledger documents never expire early | TestLedgerClaimAndComplete | internal/telemetry/dedup_integration_test.go |
| mongo-data-model | Alert record content | TestAlertingAgainstStore | internal/telemetry/alerting_integration_test.go |
| mongo-data-model | One alert per device | TestAlertingAgainstStore | internal/telemetry/alerting_integration_test.go |
| rollback-notification | An unannounced rollback is still a rollback | TestRollbackAnnouncementFailureIsRecordedAgainstItsStep | internal/temporal/rollback_workflow_test.go |
| rollback-notification | A republished announcement is the same event | TestAnnounceRollbackEventIdentityIsStable | internal/temporal/rollback_notification_test.go |
| rollback-notification | The two phases of one rollback are distinct events | TestRollbackEventIdentity | internal/telemetry/notifier_test.go |
| rollback-notification | An announcement names its contract version | TestRollbackEventRoundTrip | internal/telemetry/notifier_test.go |
| rollback-saga | The plan compensates in reverse order | TestDeriveRollbackPlan | internal/temporal/rollback_workflow_test.go |
| rollback-saga | A replay derives the same plan | TestRollbackSurvivesRestartsInEveryCompensation | cmd/worker/rollout_restart_rollback_scenarios_integration_test.go |
| rollback-saga | A wave that was never dispatched contributes no step | TestDeriveRollbackPlan | internal/temporal/rollback_workflow_test.go |
| rollback-saga | Steps run and are recorded in order | TestRollbackReportsItsProgressWithoutWaiting | internal/temporal/rollback_workflow_test.go |
| rollback-saga | A permanently failed step does not stop the plan | TestRollbackSurvivesRestartsInEveryCompensation | cmd/worker/rollout_restart_rollback_scenarios_integration_test.go |
| rollback-saga | The terminal record comes last | TestRollbackPlanRunsAndIsRecorded | internal/temporal/rollback_workflow_test.go |
| rollback-saga | A re-recorded step converges | TestStore | internal/rollout/store_integration_test.go |
| rollback-saga | The compensating phase is reported | TestRollbackReportsItsProgressWithoutWaiting | internal/temporal/rollback_workflow_test.go |
| rollback-saga | A concluded rollback reports what it achieved | TestRollbackPlanRunsAndIsRecorded | internal/temporal/rollback_workflow_test.go |
| rollback-saga | The inventory accounts for every device the rollback touched | TestRollbackBookkeepingMatchesTheDevices | cmd/worker/rollout_restart_rollback_scenarios_integration_test.go |
| rollback-saga | Control signals do nothing during a rollback | TestRollbackStateControlChangesNothing | internal/temporal/rollback_workflow_test.go |
| rollback-saga | A retried restore repeats its command id | TestDowngradeDeviceActivityIsIdempotent | internal/temporal/device_downgrade_test.go |
| rollback-saga | A retried reconciliation writes nothing | TestStore | internal/devices/store_integration_test.go |
| rollback-saga | A republished announcement is the same event | TestAnnounceRollbackEventIdentityIsStable | internal/temporal/rollback_notification_test.go |
| rollback-saga | Nothing is recorded as achieved that was not observed | TestRollbackRecordsWhatItCouldNotRestore | internal/temporal/rollback_workflow_test.go |
| rollout-api | A start request begins a rollout | TestHandlerStart | internal/rolloutapi/handler_test.go |
| rollout-api | An incomplete request is rejected | TestHandlerStart | internal/rolloutapi/handler_test.go |
| rollout-api | A running rollout reports where it stands | TestRolloutWorkflowAnswersTheStateQuery | internal/temporal/rollout_workflow_test.go |
| rollout-api | A concluded rollout reports why it ended | TestHandlerState | internal/rolloutapi/handler_test.go |
| rollout-api | An unknown rollout is not found | TestHandlerState | internal/rolloutapi/handler_test.go |
| rollout-api | An approval is delivered | TestRolloutSmoke | cmd/worker/rollout_integration_test.go |
| rollout-api | A pause and a resume are delivered | TestRolloutSmoke | cmd/worker/rollout_integration_test.go |
| rollout-api | Commanding a concluded rollout is refused | TestHandlerCommands | internal/rolloutapi/handler_test.go |
| rollout-api | Commanding an unknown rollout is not found | TestHandlerCommands | internal/rolloutapi/handler_test.go |
| rollout-api | Both APIs answer on one listener | TestGatewayHandler | cmd/controlplane/main_test.go |
| rollout-api | An unsupported method is rejected | TestHandlerRoutes | internal/rolloutapi/handler_test.go |
| rollout-api | An unknown path is rejected | TestHandlerRoutes | internal/rolloutapi/handler_test.go |
| rollout-control | A pause stops the rollout at the next wave boundary | TestRolloutPauseSurvivesARestart | cmd/worker/rollout_restart_scenarios_integration_test.go |
| rollout-control | A resume continues where the rollout stopped | TestRolloutWorkflowPauseStopsAtTheNextWaveBoundary | internal/temporal/rollout_control_test.go |
| rollout-control | Pausing a rollout that waits for approval keeps the approval | TestRolloutWorkflowPauseHoldsApprovalWaitAndKeepsTheApproval | internal/temporal/rollout_control_test.go |
| rollout-control | Repeated signals change nothing | TestRolloutWorkflowControlSignalsAreIdempotent | internal/temporal/rollout_control_test.go |
| rollout-control | A concluded rollout ignores pause and resume | TestRolloutWorkflowControlSignalsAreIdempotent | internal/temporal/rollout_control_test.go |
| rollout-control | A pause does not suppress the wave in flight | TestRolloutPauseSurvivesARestart | cmd/worker/rollout_restart_scenarios_integration_test.go |
| rollout-control | A regression still rolls back a paused rollout | TestRolloutWorkflowRegressionRollsBackAPausedRollout | internal/temporal/rollout_control_test.go |
| rollout-control | A healthy wave does not advance a paused rollout | TestRolloutPauseSurvivesARestart | cmd/worker/rollout_restart_scenarios_integration_test.go |
| rollout-control | A pause during a window is applied without moving the window | TestRolloutWorkflowPauseInsideAWindowKeepsTheWindowDeadline | internal/temporal/rollout_control_test.go |
| rollout-control | A pause survives a worker restart | TestRolloutPauseSurvivesARestart | cmd/worker/rollout_restart_scenarios_integration_test.go |
| rollout-control | An approval arriving while paused is banked | TestRolloutSmoke | cmd/worker/rollout_integration_test.go |
| rollout-durability | A restart between two waves resumes with the next wave | TestRolloutRestartsAcrossEveryActivity | cmd/worker/rollout_restart_scenarios_integration_test.go |
| rollout-durability | A restart inside a health window does not move the window | TestRolloutPauseSurvivesARestart | cmd/worker/rollout_restart_scenarios_integration_test.go |
| rollout-durability | A restart between the approval and the gated wave | TestRolloutRestartsAcrossEveryActivity | cmd/worker/rollout_restart_scenarios_integration_test.go |
| rollout-durability | A restart while paused mid-wave | TestRolloutPauseSurvivesARestart | cmd/worker/rollout_restart_scenarios_integration_test.go |
| rollout-durability | A restart inside the rollback resumes the plan where it stopped | TestRollbackSurvivesRestartsInEveryCompensation | cmd/worker/rollout_restart_rollback_scenarios_integration_test.go |
| rollout-durability | A concluded rollout stays concluded | TestRolloutRestartsAcrossEveryActivity | cmd/worker/rollout_restart_scenarios_integration_test.go |
| rollout-durability | A completed activity is not executed again | TestRolloutRestartsAfterEveryEffect | cmd/worker/rollout_restart_scenarios_integration_test.go |
| rollout-durability | An effect applied before a failed attempt is not applied twice | TestRolloutRestartsAfterEveryEffect | cmd/worker/rollout_restart_scenarios_integration_test.go |
| rollout-durability | The effect ledger stays exactly-once across the restart matrix | TestRolloutRestartsAfterEveryEffect | cmd/worker/rollout_restart_scenarios_integration_test.go |
| rollout-health-api | A wave's health is returned for a rollout and wave | TestGetWaveHealthServesTheEvaluation | internal/wavehealth/service_test.go |
| rollout-health-api | Identifiers are echoed | TestGetWaveHealthServesTheEvaluation | internal/wavehealth/service_test.go |
| rollout-health-api | The verdict set is closed | TestGetWaveHealthServesTheEvaluation | internal/wavehealth/service_test.go |
| rollout-health-api | Empty wave id is rejected | TestGetWaveHealthInvalidRequestRunsNoQuery | internal/wavehealth/service_test.go |
| rollout-health-api | Empty rollout id is rejected | TestGetWaveHealthInvalidRequestRunsNoQuery | internal/wavehealth/service_test.go |
| rollout-health-api | Unknown rollout is not found | TestGetWaveHealthStatusMapping | internal/wavehealth/service_test.go |
| rollout-health-api | Wave of another rollout is not found | TestGetWaveHealthServesAHandInsertedWave | cmd/controlplane/wavehealth_integration_test.go |
| rollout-health-api | Storage failure leaks nothing | TestGetWaveHealthLeaksNothing | internal/wavehealth/service_test.go |
| rollout-health-api | Renumbering is rejected | TestFrozenFields | api/proto/rollout/v1/rollout_test.go |
| rollout-health-api | The agent contract stays untouched | TestAgentContractStaysUntouched | api/proto/rollout/v1/rollout_test.go |
| rollout-health-gate | The window opens at the wave's start | TestRolloutGateWaitsTheDurableWindow | internal/temporal/rollout_gate_test.go |
| rollout-health-gate | A worker restart mid-window resumes the remainder | TestRolloutGateWaitsTheDurableWindow | internal/temporal/rollout_gate_test.go |
| rollout-health-gate | An elapsed window is not re-waited | TestRolloutGateWaitsTheDurableWindow | internal/temporal/rollout_gate_test.go |
| rollout-health-gate | Only the wave's recorded devices are counted | TestEvaluateQueriesTheConfiguredWindow | internal/wavehealth/wavehealth_test.go |
| rollout-health-gate | The evaluation moment is the workflow's decision time | TestRolloutGateRemeasuresAnUndecidedWave | internal/temporal/rollout_gate_test.go |
| rollout-health-gate | A failed evaluation is retried rather than treated as a regression | TestRolloutGateRetriesAFailedEvaluation | internal/temporal/rollout_gate_test.go |
| rollout-health-gate | A healthy wave is promoted | TestRolloutWorkflowDrivesTheSequenceInOrder | internal/temporal/rollout_workflow_test.go |
| rollout-health-gate | Health at the boundary promotes | TestRolloutGateDecidesOnTheVerdict | internal/temporal/rollout_gate_test.go |
| rollout-health-gate | An unhealthy wave rolls the rollout back | TestRolloutGateDecidesOnTheVerdict | internal/temporal/rollout_gate_test.go |
| rollout-health-gate | An undecided wave is measured again, not promoted | TestRolloutGateRemeasuresAnUndecidedWave | internal/temporal/rollout_gate_test.go |
| rollout-health-gate | A wave that never gathers evidence is treated as unhealthy | TestRolloutGateTreatsANeverDecidedWaveAsUnhealthy | internal/temporal/rollout_gate_test.go |
| rollout-health-gate | The rollback names the failing wave | TestRolloutGateRollsBackAndLeavesPromotedWavesAlone | internal/temporal/rollout_gate_test.go |
| rollout-health-gate | No wave starts after a rollback | TestRolloutGateRollsBackAndLeavesPromotedWavesAlone | internal/temporal/rollout_gate_test.go |
| rollout-health-gate | Promoted waves keep their outcome | TestRolloutGateRollsBackAndLeavesPromotedWavesAlone | internal/temporal/rollout_gate_test.go |
| rollout-health-gate | A gated wave starts only after approval | TestRolloutGateHoldsAGatedWaveUntilApproval | internal/temporal/rollout_gate_test.go |
| rollout-health-gate | An approval lets the gated wave start | TestRolloutGateHoldsAGatedWaveUntilApproval | internal/temporal/rollout_gate_test.go |
| rollout-health-gate | One approval passes one gated wave | TestRolloutGateSpendsOneApprovalPerGatedWave | internal/temporal/rollout_gate_test.go |
| rollout-health-gate | An early approval is held and reported | TestRolloutGateBanksAnEarlyApproval | internal/temporal/rollout_gate_test.go |
| rollout-health-gate | Approval survives a worker restart | TestRolloutRestartsAcrossEveryActivity | cmd/worker/rollout_restart_scenarios_integration_test.go |
| rollout-health-gate | A concluded rollout ignores approval | TestRolloutStateTransitions | internal/temporal/rollout_test.go |
| rollout-search-attributes | Attribute values mirror rollout state | TestRolloutWorkflowSearchAttributes | internal/temporal/rollout_attrs_test.go |
| rollout-search-attributes | An unknown firmware version stays empty | TestRolloutWorkflowSearchAttributes | internal/temporal/rollout_attrs_test.go |
| rollout-search-attributes | Loading the firmware fills the firmware attribute | TestRolloutWorkflowSearchAttributes | internal/temporal/rollout_attrs_test.go |
| rollout-search-attributes | Waiting for approval changes the status attribute | TestRolloutWorkflowSearchAttributes | internal/temporal/rollout_attrs_test.go |
| rollout-search-attributes | Pausing changes the status attribute | TestRolloutWorkflowSearchAttributes | internal/temporal/rollout_attrs_test.go |
| rollout-search-attributes | Concluding changes the status attribute | TestRolloutWorkflowSearchAttributes | internal/temporal/rollout_attrs_test.go |
| rollout-search-attributes | Fresh namespace gets the attributes | TestBootstrapNamespace | cmd/worker/main_test.go |
| rollout-search-attributes | Re-registration is a no-op | TestEnsureSearchAttributes | internal/temporal/searchattrs_test.go |
| rollout-workflow | Every target device receives its command | TestRolloutReachesARealAgent | cmd/controlplane/rollout_agent_join_integration_test.go |
| rollout-workflow | A wave carries its device outcomes | TestRolloutReachesARealAgent | cmd/controlplane/rollout_agent_join_integration_test.go |
| temporal-worker | Startup registration | TestWorkerRegistersTheWholeWorkQueue | cmd/worker/main_test.go |
| temporal-worker | Registration names are explicit | TestWorkerRegistersTheWholeWorkQueue | cmd/worker/main_test.go |
| temporal-worker | The per-device update activity is registered | TestRegisterRollout | cmd/worker/main_test.go |
| temporal-worker | The rollback activities are registered | TestRegisterRollout | cmd/worker/main_test.go |
| temporal-worker | The announcement publisher is hosted by the worker | TestStartNotifier | cmd/worker/main_test.go |
| temporal-worker | A broker outage does not stop an announcement | TestNotifierAgainstBroker | internal/telemetry/rollback_notification_integration_test.go |
| temporal-worker | Agent transport stays in the control plane | TestRegisterRollout | cmd/worker/main_test.go |
| temporal-worker | Rollout work is shared by worker replicas | TestRolloutRestartsAcrossEveryActivity | cmd/worker/rollout_restart_scenarios_integration_test.go |
| temporal-worker | Bootstrap precedes task polling | TestBootstrapNamespace | cmd/worker/main_test.go |
| temporal-worker | The rollout attributes are bootstrapped too | TestBootstrapNamespace | cmd/worker/main_test.go |
| temporal-worker | Concurrent replica bootstrap is harmless | TestEnsureSearchAttributes | internal/temporal/searchattrs_test.go |
| temporal-worker | The control plane never polls the work queue | TestStartWorker | cmd/controlplane/main_test.go |
| temporal-worker | The control plane registers the dispatch activity | TestStartWorker | cmd/controlplane/main_test.go |
| temporal-worker | Device commands are scheduled on the control-plane queue | TestDeviceWorkflowDispatchTaskQueue | internal/temporal/device_workflow_test.go |
| temporal-worker | A device command still reaches its device | TestRolloutReachesARealAgent | cmd/controlplane/rollout_agent_join_integration_test.go |
| temporal-worker | The eligible-device watcher runs in the worker | TestStartPoolWatcher | cmd/worker/watcher_test.go |
| temporal-worker | The control plane registers the firmware workflow and its upload activities | TestStartWorker | cmd/controlplane/main_test.go |
| temporal-worker | Firmware upload tasks stay on the control-plane queue | TestStartWorker | cmd/controlplane/main_test.go |
| agent-fault-injection | A configured fault share produces faults | TestFaultFetcherShareBounds | internal/agent/fault_test.go |
| agent-fault-injection | No configuration means no faults | TestFaultFetcherZeroConfigDelegates | internal/agent/fault_test.go |
| agent-fault-injection | A stalled download is bounded by the context | TestUpdateHandlerStallEndsWithTheCommandContext | internal/agent/retry_test.go |
| agent-fault-injection | The same seed replays the same faults | TestFaultFetcherSameSeedReplaysTheSameFaults | internal/agent/fault_test.go |
| agent-fault-injection | Fault decisions are per device | TestFaultFetcherDecisionsArePerDevice | internal/agent/fault_test.go |
| agent-fault-injection | A transient fault succeeds on a retry | TestUpdateHandlerRetriesTheFetch | internal/agent/retry_test.go |
| agent-fault-injection | Backoff grows and is capped | TestUpdateHandlerRetriesTheFetch | internal/agent/retry_test.go |
| agent-fault-injection | Exhausting the budget reports the failure | TestUpdateHandlerRetriesTheFetch | internal/agent/retry_test.go |
| agent-fault-injection | A checksum mismatch is not retried | TestUpdateHandlerRetriesTheFetch | internal/agent/retry_test.go |
| agent-fault-injection | Shutdown interrupts the retry loop | TestUpdateHandlerStopsOnCancellationDuringARetryWait | internal/agent/retry_test.go |
| agent-fault-injection | A fault that outlasts its retries is reported as a failure | TestUpdateHandlerRetriesInjectedFaults | internal/agent/retry_test.go |
| agent-fault-injection | A poison event reaches the dead-letter queue | TestFaultedReportReachesTheDeadLetterQueue | internal/telemetry/faulted_report_dlq_integration_test.go |
| device-eligibility-pool | A new device enters its selector's view | TestPoolViewTracksDeviceChanges | internal/rollout/pool_integration_test.go |
| device-eligibility-pool | A device that changes model moves between views | TestPoolViewTracksDeviceChanges | internal/rollout/pool_integration_test.go |
| device-eligibility-pool | A device that leaves the fleet leaves the view | TestPoolViewTracksDeviceChanges | internal/rollout/pool_integration_test.go |
| device-eligibility-pool | A change in a later wave does not move an earlier wave's membership | TestResolutionReadsTheView | internal/rollout/store_view_integration_test.go |
| device-eligibility-pool | Startup derivation matches a full read | TestPoolViewDerivesFromDevices | internal/rollout/pool_integration_test.go |
| device-eligibility-pool | A redelivered change does not duplicate an entry | TestPoolViewFoldIsIdempotent | internal/rollout/pool_test.go |
| device-eligibility-pool | Reconnection resumes from the last position | TestPoolViewResumesFromItsToken | internal/rollout/pool_integration_test.go |
| device-eligibility-pool | An unrecoverable position triggers a rebuild | TestPoolViewFlushesAnUnusablePosition | internal/rollout/pool_integration_test.go |
| device-eligibility-pool | Shutdown releases the stream | TestPoolViewStopsWithItsContext | internal/rollout/pool_integration_test.go |
| device-eligibility-pool | A wave resolves against the maintained view | TestResolutionReadsTheView | internal/rollout/store_view_integration_test.go |
| device-eligibility-pool | An empty selector still records an empty wave | TestResolutionOnAnEmptySelector | internal/rollout/store_view_integration_test.go |
| firmware-workflow | Starting an upload records it as accepted | TestFirmwareWorkflowUploadsOneFirmware | internal/temporal/firmware_workflow_test.go |
| firmware-workflow | A retried upload reaches the same execution | TestFirmwareUploadStarter | internal/temporal/firmware_uploads_test.go |
| firmware-workflow | A concluded upload is terminal | TestFirmwareUploadStateTransitions | internal/temporal/firmware_state_test.go |
| firmware-workflow | Validation precedes storage | TestFirmwareWorkflowRefusesUnknownModels | internal/temporal/firmware_workflow_test.go |
| firmware-workflow | A retried storage step stores one binary | TestStoreBinary | internal/firmware/upload_steps_test.go |
| firmware-workflow | A version conflict fails the upload without touching the existing record | TestFirmwareWorkflowFailsAVersionConflict | internal/temporal/firmware_workflow_test.go |
| firmware-workflow | A failed metadata write leaves no orphaned binary | TestFirmwareWorkflowDiscardsAnUnrecordableUpload | internal/temporal/firmware_workflow_test.go |
| firmware-workflow | State transitions are recorded | TestFirmwareWorkflowUploadsOneFirmware | internal/temporal/firmware_workflow_test.go |
| firmware-workflow | A failed upload carries its reason | TestFirmwareWorkflowRecordsAStorageFailure | internal/temporal/firmware_workflow_test.go |
| firmware-workflow | A retried transition does not regress the record | TestUpsertMetadata | internal/firmware/upload_steps_test.go |
| firmware-workflow | A running upload reports its progress | TestFirmwareWorkflowServesItsStateWhileStoring | internal/temporal/firmware_workflow_test.go |
| firmware-workflow | A concluded upload reports its outcome | TestFirmwareUploadStates | internal/temporal/firmware_uploads_test.go |
| firmware-workflow | A failed upload reports why | TestFirmwareWorkflowRefusesUnknownModels | internal/temporal/firmware_workflow_test.go |
| functional-requirement-coverage | The coverage check reports a newly unbacked scenario | TestCheck | internal/speccoverage/speccoverage_test.go |
| functional-requirement-coverage | An exemption is named rather than inferred | TestCheck | internal/speccoverage/speccoverage_test.go |
| functional-requirement-coverage | A reader follows a scenario to its test | TestCheck | internal/speccoverage/speccoverage_test.go |
| functional-requirement-coverage | An added scenario without a test is caught | TestCoverageLedger | internal/speccoverage/speccoverage_test.go |
| functional-requirement-coverage | A stack-dependent scenario is exempt with its blocker named | TestCheck | internal/speccoverage/speccoverage_test.go |
| functional-requirement-coverage | An exemption is refused for a scenario a test could verify | TestCheck | internal/speccoverage/speccoverage_test.go |
| runtime-config | Absent fields take defaults | TestLoad | internal/config/config_test.go |
| runtime-config | Present fields override defaults | TestLoad | internal/config/config_test.go |
| runtime-config | Liveness, telemetry, and snapshots sections default | TestLoad | internal/config/config_test.go |
| runtime-config | The dispatch task queue defaults to its own queue | TestTemporalQueueDefaults | internal/config/config_test.go |
| runtime-config | No config flag loads defaults only | TestLoadWithoutPathUsesDefaults | internal/config/config_test.go |
| runtime-config | Named config file is required to exist | TestLoadMissingFile | internal/config/config_test.go |
| runtime-config | Broker and alerting sections default | TestBrokerAndAlertingDefaults | internal/config/config_test.go |
| runtime-config | Rollout gating section defaults | TestRolloutDefaults | internal/config/config_test.go |
| runtime-config | Rollout wave sequence defaults | TestRolloutDefaults | internal/config/config_test.go |
| runtime-config | A configured wave sequence overrides the default | TestLoad | internal/config/config_test.go |
| runtime-config | Liveness and telemetry sections default | TestLoad | internal/config/config_test.go |
| runtime-config | Unknown key is rejected | TestLoad | internal/config/config_test.go |
| runtime-config | Malformed value is rejected | TestLoad | internal/config/config_test.go |
| runtime-config | Invalid duration or scale value is rejected | TestValidate | internal/config/validate_test.go |
| runtime-config | An empty queue name is rejected | TestValidate | internal/config/validate_test.go |
| runtime-config | Valid partial file passes | TestLoad | internal/config/config_test.go |
| runtime-config | Invalid broker pipeline value is rejected | TestValidate | internal/config/validate_test.go |
| runtime-config | Invalid rollout gating value is rejected | TestValidate | internal/config/validate_test.go |
| runtime-config | Invalid rollout result timeout is rejected | TestValidate | internal/config/validate_test.go |
| runtime-config | Invalid rollout wave sequence is rejected | TestValidate | internal/config/validate_test.go |
| runtime-config | Invalid liveness or telemetry value is rejected | TestValidate | internal/config/validate_test.go |
| runtime-config | Agent emulator scale comes from configuration | TestRunRegistersTheConfiguredFleetSize | cmd/agent/main_test.go |
| runtime-config | Both queues come from configuration | TestStartWorker | cmd/controlplane/main_test.go |
| runtime-config | Event pipeline behaviour comes from configuration | TestEventPipelineEndToEnd | cmd/controlplane/pipeline_integration_test.go |
| runtime-config | Wave health gating comes from configuration | TestGetWaveHealthServesAHandInsertedWave | cmd/controlplane/wavehealth_integration_test.go |
| runtime-config | Rollout sequence and gate timing come from configuration | TestRolloutSettings | cmd/controlplane/main_test.go |
| runtime-config | Rollout result waiting comes from configuration | TestRolloutSettings | cmd/controlplane/main_test.go |
| agent-emulator | Emission is periodic | TestFleetEmitsOnTheConfiguredPeriod | internal/agent/fleet_test.go |
| broker-observability | Dropped events are counted with a reason | TestPublisherCountsBrokerRefusals | internal/telemetry/publisher_test.go |
| rollout-workflow | Starting a rollout records it as running | TestRolloutWorkflowDrivesTheSequenceInOrder | internal/temporal/rollout_workflow_test.go |
| rollout-workflow | One workflow drives a rollout | TestRolloutStarterStart | internal/temporal/rollout_starter_test.go |
| rollout-workflow | The selector does not move under a running rollout | TestResolutionReadsTheView | internal/rollout/store_view_integration_test.go |
| rollout-workflow | A concluded rollout is terminal | TestRolloutRestartsAcrossEveryActivity | cmd/worker/rollout_restart_scenarios_integration_test.go |
| rollout-workflow | Waves run in sequence order | TestRolloutWorkflowDrivesTheSequenceInOrder | internal/temporal/rollout_workflow_test.go |
| rollout-workflow | Only one wave is in flight | TestRolloutWorkflowDrivesTheSequenceInOrder | internal/temporal/rollout_workflow_test.go |
| rollout-workflow | Exhausting the sequence completes the rollout | TestRolloutWorkflowDrivesTheSequenceInOrder | internal/temporal/rollout_workflow_test.go |
| rollout-workflow | Each wave targets its own added share | TestWaveTargets | internal/rollout/wave_test.go |
| rollout-workflow | A retried resolution converges | TestStore | internal/rollout/store_integration_test.go |
| rollout-workflow | A share that resolves to no device is recorded and skipped | TestRolloutWorkflowSkipsAWaveThatTargetsNobody | internal/temporal/rollout_workflow_test.go |
| rollout-workflow | The wave waits for its devices' results | TestRolloutWorkflowUnreportedDeviceStopsTheWait | internal/temporal/rollout_deviceoutcomes_test.go |
| rollout-workflow | A device that never reports stops the wait | TestRolloutWorkflowUnreportedDeviceStopsTheWait | internal/temporal/rollout_deviceoutcomes_test.go |
| rollout-workflow | Mixed device outcomes are recorded | TestRolloutWorkflowRecordsPerDeviceOutcomes | internal/temporal/rollout_deviceoutcomes_test.go |
| rollout-workflow | Redelivery repeats the command id | TestUpdateDeviceActivityRetriesRepeatTheCommandID | internal/temporal/device_update_test.go |
| rollout-workflow | Incompatible firmware fails the rollout at start | TestRolloutWorkflowCannotStart | internal/temporal/rollout_workflow_test.go |
| rollout-workflow | A wave that cannot be delivered is not promoted | TestRolloutWorkflowRollsBackOnDispatchFailure | internal/temporal/rollout_workflow_test.go |
| rollout-workflow | Status transitions are recorded | TestRolloutWorkflowControlSignalsAreIdempotent | internal/temporal/rollout_control_test.go |
| rollout-workflow | A compensating rollout is recorded while it compensates | TestRollbackPlanRunsAndIsRecorded | internal/temporal/rollback_workflow_test.go |
| rollout-workflow | A concluded rollback carries what it achieved | TestRolloutSmoke | cmd/worker/rollout_integration_test.go |
| rollout-workflow | A decided wave carries its measured health | TestRolloutGateDecidesOnTheVerdict | internal/temporal/rollout_gate_test.go |
| rollout-workflow | A retried write does not regress the record | TestRolloutWorkflowRecordsRetriedWrites | internal/temporal/rollout_workflow_test.go |
| rollout-workflow | A running rollout reports its position | TestRolloutWorkflowAnswersTheStateQuery | internal/temporal/rollout_workflow_test.go |
| rollout-workflow | A wave's device outcomes are reported | TestRolloutWorkflowRecordsPerDeviceOutcomes | internal/temporal/rollout_deviceoutcomes_test.go |
| rollout-workflow | A waiting rollout reports what it waits for | TestRolloutGateHoldsAGatedWaveUntilApproval | internal/temporal/rollout_gate_test.go |
| rollout-workflow | A compensating rollout answers without waiting for its compensations | TestRollbackReportsItsProgressWithoutWaiting | internal/temporal/rollback_workflow_test.go |
| rollout-workflow | A concluded rollout reports why | TestRolloutGateRollsBackAndLeavesPromotedWavesAlone | internal/temporal/rollout_gate_test.go |
| agent-firmware-apply | Shutdown interrupts an in-flight application | TestApplierInterruptsPromptly | internal/agent/apply_test.go |
| temporal-worker | A workflow added to the work queue cannot be bounced | TestStartWorker | cmd/controlplane/main_test.go |
| device-registry | Heartbeat from an unregistered device changes nothing | TestUnenrolledHeartbeatNotRouted | internal/agentserver/server_test.go |
| mongo-data-model | Target models are recorded | TestUploadStepsOnMongo | internal/firmware/upload_steps_integration_test.go |
| mongo-data-model | A concluded rollback carries what it achieved | TestRollbackPlanRunsAndIsRecorded | internal/temporal/rollback_workflow_test.go |
| mongo-data-model | A wave records the devices that did not take the update | TestStore | internal/rollout/store_integration_test.go |
| mongo-data-model | A wave that targets no devices is still a record | TestStore | internal/rollout/store_integration_test.go |
| mongo-data-model | Wave status reflects the wave's life | TestRolloutWorkflowDrivesTheSequenceInOrder | internal/temporal/rollout_workflow_test.go |
| mongo-data-model | Old telemetry expires automatically | TestSchema | internal/mongotest/schema_test.go |
| device-update | The wait survives an attempt that dies | TestRolloutRestartsAfterEveryEffect | cmd/worker/rollout_restart_scenarios_integration_test.go |
| device-update | A retried attempt repeats the command id | TestUpdateDeviceActivityRetriesRepeatTheCommandID | internal/temporal/device_update_test.go |
| wave-health-aggregation | Ratio and sample size over the wave's devices | TestEvaluate | internal/wavehealth/wavehealth_test.go |
| wave-health-aggregation | Only the wave's target devices count | TestEvaluate | internal/wavehealth/wavehealth_test.go |
| wave-health-aggregation | Redelivered heartbeats are counted once | TestStore | internal/wavehealth/store_integration_test.go |
| wave-health-aggregation | Samples older than the window age out | TestEvaluate | internal/wavehealth/wavehealth_test.go |
| wave-health-aggregation | The window never reaches back before the wave started | TestEvaluate | internal/wavehealth/wavehealth_test.go |
| wave-health-aggregation | The window is reported with the result | TestEvaluate | internal/wavehealth/wavehealth_test.go |
| wave-health-aggregation | Exactly at the boundary is healthy | TestBoundaryIsInclusive | internal/wavehealth/wavehealth_test.go |
| wave-health-aggregation | One failing sample across the boundary flips the verdict | TestBoundaryIsInclusive | internal/wavehealth/wavehealth_test.go |
| wave-health-aggregation | Too few samples cannot be healthy | TestTooFewSamplesCannotBeHealthy | internal/wavehealth/wavehealth_test.go |
| wave-health-aggregation | A brief dip leaves a healthy wave healthy | TestTransientFailuresDoNotReadAsRegression | internal/wavehealth/wavehealth_test.go |
| wave-health-aggregation | Sustained degradation crosses the boundary | TestTransientFailuresDoNotReadAsRegression | internal/wavehealth/wavehealth_test.go |
| wave-health-aggregation | Recovery is visible once the dip ages out | TestTransientFailuresDoNotReadAsRegression | internal/wavehealth/wavehealth_test.go |
| wave-health-aggregation | A wave without samples is undecided | TestEvaluate | internal/wavehealth/wavehealth_test.go |
| wave-health-aggregation | A wave without target devices is undecided | TestStore | internal/wavehealth/store_integration_test.go |
| wave-health-aggregation | An empty window is not a failure | TestEvaluate | internal/wavehealth/wavehealth_test.go |
| rollback-notification | A rollback is announced before it compensates | TestRollbackPlanRunsAndIsRecorded | internal/temporal/rollback_workflow_test.go |
| rollback-notification | A rollback is announced when its compensations have run | TestRollbackPlanRunsAndIsRecorded | internal/temporal/rollback_workflow_test.go |
| rollback-notification | A consumer can narrow to rollback announcements | TestRollbackNotificationsAgainstBroker | internal/telemetry/rollback_notification_integration_test.go |
| rollback-notification | The step completes only once the broker accepted the event | TestRollbackAnnouncementFailureIsRecordedAgainstItsStep | internal/temporal/rollback_workflow_test.go |
| device-workflow | Workflow state wins over stored copies | TestRollbackBookkeepingMatchesTheDevices | cmd/worker/rollout_restart_rollback_scenarios_integration_test.go |
| device-workflow | History stays bounded under sustained load | TestDeviceWorkflowRollingContinuation | internal/temporal/device_workflow_test.go |
| heartbeat-consumption | In-flight work is bounded | TestConsumerServeBoundsInFlightWorkAndStopsOnLoss | internal/telemetry/consumer_test.go |
| heartbeat-consumption | Consumer resumes after a broker restart | TestEventPipelineEndToEnd | cmd/controlplane/pipeline_integration_test.go |
| heartbeat-consumption | Redelivery after reconnect is deduplicated | TestConsumerAcknowledgesDuplicatesWithoutApplying | internal/telemetry/consumer_test.go |
| heartbeat-event-publication | Unroutable event is reported, not swallowed | TestUnroutablePublishIsReturnedAndCountable | internal/telemetry/publisher_integration_test.go |
| telemetry-ingest | Heartbeat lands with timestamp and meta | TestWriterIngest | internal/telemetry/writer_integration_test.go |
| telemetry-ingest | Meta comes from the registered identity | TestWriterIngest | internal/telemetry/writer_integration_test.go |
| telemetry-ingest | Missing measurement time is not stored | TestWriterRefusesHeartbeatWithoutMeasurementTime | internal/telemetry/writer_test.go |
| telemetry-ingest | Redelivered event is a no-op | TestWriterIngest | internal/telemetry/writer_integration_test.go |
| telemetry-ingest | Mixed batch succeeds partially | TestWriterIngest | internal/telemetry/writer_integration_test.go |
| telemetry-ingest | Steady heartbeats are written in batches | TestWriterIngest | internal/telemetry/writer_integration_test.go |
| telemetry-ingest | Quiet fleet still persists promptly | TestWriterFlushesOnInterval | internal/telemetry/writer_test.go |
| telemetry-ingest | Slow database does not grow unbounded memory | TestWriterBackpressureBlocksInsteadOfBuffering | internal/telemetry/writer_test.go |
| device-liveness | Connected but silent is still marked offline | TestPersistencePathEndToEnd | internal/agentserver/persistence_integration_test.go |
| device-liveness | Threshold drives detection | TestSweeperPassesStaleCutoff | internal/devices/sweeper_test.go |
| repo-scaffold | Fresh clone exposes the layout | TestFreshCloneExposesTheLayout | internal/repolayout/layout_test.go |
| repo-scaffold | No code outside the sanctioned directories | TestNoCodeOutsideSanctionedDirectories | internal/repolayout/layout_test.go |
| repo-scaffold | Planned paths are not presented as working | TestPlannedPathsAreMarkedNotImplemented | internal/repolayout/layout_test.go |
| repo-scaffold | Behavior is testable below the entrypoint | TestBehaviorIsTestableBelowTheEntrypoint | internal/repolayout/layout_test.go |
| repo-scaffold | Definition of Done via the build tool | TestCheckRunsTheDefinitionOfDoneInOrder | internal/repolayout/makefile_test.go |
| repo-scaffold | Single targets stay single-purpose | TestSingleTargetsRemainSinglePurpose | internal/repolayout/makefile_test.go |
| repo-scaffold | Usable clone without submodule content | TestWebIsPlaceholderOnly | internal/repolayout/submodule_test.go |
| repo-scaffold | Submodule registration is explicit | TestFrontendSubmoduleRegistrationIsExplicit | internal/repolayout/submodule_test.go |
| proto-codegen | Predictable import path | TestContractLayout | internal/protocodegen/contract_test.go |
| proto-codegen | One package per bounded context | TestContextsAreIndependent | internal/protocodegen/contexts_test.go |
| proto-codegen | Reproducible across machines | TestRegenerationReproducesCommittedStubs | internal/protocodegen/regen_test.go |
| proto-codegen | Tools are not runtime dependencies | TestGeneratorPluginsAreBuildOnly | internal/protocodegen/tooling_test.go |
| proto-codegen | Regenerating the full stub surface | TestRegenerationReproducesCommittedStubs | internal/protocodegen/regen_test.go |
| proto-codegen | Missing toolchain fails loudly | TestMissingToolchainFailsLoudly | internal/protocodegen/missingtool_test.go |
| proto-codegen | Regeneration is idempotent | TestRegenerationIsIdempotent | internal/protocodegen/regen_test.go |
| proto-codegen | Hand edits do not survive | TestHandEditsDoNotSurviveRegeneration | internal/protocodegen/regen_test.go |
| proto-codegen | Review sees contracts first | TestContractLayout | internal/protocodegen/contract_test.go |
| test-execution-profiles | The profile comes from the environment | TestGetHonoursTheProcessEnvironment | internal/testexec/profile_test.go |
| test-execution-profiles | An undocumented variable changes nothing | TestResolveFrom | internal/testexec/profile_test.go |
| test-execution-profiles | An unconfigured run is today's run | TestResolveFrom | internal/testexec/profile_test.go |
| test-execution-profiles | A configured run narrows, not widens | TestWeakProfileNarrowsEveryKnob | internal/testexec/profile_test.go |
| test-execution-profiles | An unaffordable test says so | TestRequireFleetSize | internal/testexec/profile_test.go |
| test-execution-profiles | The same environment behaves the same on any host | TestResolveIgnoresTheHostShape | internal/testexec/profile_test.go |
| test-execution-profiles | A resource-shaped run is an explicit choice | TestResolveIgnoresTheHostShape | internal/testexec/profile_test.go |
| test-execution-profiles | An unknown profile name fails the run | TestMainFailsThePackageOnAnUnusableEnvironment | internal/testexec/profile_test.go |
| test-execution-profiles | A non-positive size or budget fails the run | TestResolveFrom | internal/testexec/profile_test.go |
| test-execution-profiles | One stack serves a package's tests | TestSharedGivesEveryTestItsOwnDatabase | internal/mongotest/shared_integration_test.go |
| test-execution-profiles | Tests stay isolated from each other | TestSharedGivesEveryTestItsOwnDatabase | internal/mongotest/shared_integration_test.go |
| test-execution-profiles | A stack failure fails the package, not one test | TestMainFailsThePackageWhenAStackCannotStart | internal/testexec/profile_test.go |
| agent-stream-server | Complete registration is accepted | TestRegistrationAccepted | internal/agentserver/server_test.go |
| agent-stream-server | Enrolled heartbeats reach the sink unchanged | TestHeartbeatRoutedUnchanged | internal/agentserver/server_test.go |
| agent-stream-server | Repeated idempotency key is accepted with no second effect | TestRolloutReachesARealAgent | cmd/controlplane/rollout_agent_join_integration_test.go |
| agent-stream-server | Redelivered heartbeats keep their identity end to end | TestHeartbeatsSignalDeviceWorkflow | internal/agentserver/server_test.go |
| rollout-durability | The recorded state converges instead of accumulating | TestRolloutRestartsAfterEveryEffect | cmd/worker/rollout_restart_scenarios_integration_test.go |
| runtime-config | Device workflow cadence comes from configuration | TestDeviceSettingsFromConfiguration | cmd/controlplane/config_wiring_test.go |
| runtime-config | Probe listener comes from configuration | TestProbeListenerFromConfiguration | cmd/controlplane/config_wiring_test.go |
| runtime-config | Invalid configuration starts no listener | TestRejectedConfigurationStartsNoListener | cmd/controlplane/config_wiring_test.go |
| functional-requirement-coverage | A removed behavior fails its backing test | TestRemovedBehaviorFailsItsBackingTest | internal/speccoverage/mutation_test.go |
| test-execution-profiles | A narrowed run asserts the same behavior | TestNarrowedRunAssertsTheSameBehavior | internal/testexec/narrowing_test.go |
| temporal-worker | Probes answer per replica | TestReplicaProbesAnswerOnConfiguredHealthAddr | cmd/worker/main_test.go |
| temporal-worker | The worker never polls the control-plane queue | TestWorkerPollsOnlyTheConfiguredWorkQueue | cmd/worker/main_test.go |
| agent-emulator | Identity is stable across heartbeats | TestDeviceIdentityIsStableAcrossHeartbeats | internal/agent/identity_test.go |
| agent-stream-client | A dead connection becomes a reconnect | TestKeepaliveTurnsASilentlyDeadConnectionIntoAReconnect | internal/agent/dial_test.go |
| agent-control-plane-api | Adding a field keeps old clients working | TestAgentAddingFieldKeepsOldClientsWorking | internal/protocodegen/evolution_test.go |
| agent-control-plane-api | Adding the field and RPC keeps old clients working | TestAgentAddingFieldAndRPCKeepsOldClientsWorking | internal/protocodegen/evolution_test.go |
| agent-control-plane-api | Renumbering is rejected | TestRenumberingIsRejected | internal/protocodegen/renumbering_test.go |
| rollout-health-api | Adding a field keeps old clients working | TestRolloutAddingFieldKeepsOldClientsWorking | internal/protocodegen/evolution_test.go |
| device-liveness | Returning device goes online | TestWriterMarksReturningDeviceOnline | internal/telemetry/writer_ingest_integration_test.go |
| device-registry | Firmware change is picked up from a heartbeat | TestWriterHeartbeatRecordsTheReportedFirmware | internal/telemetry/writer_ingest_integration_test.go |
| heartbeat-consumption | Acknowledgement after the write | TestConsumerAcknowledgesOnlyAfterTheDurableWrite | internal/telemetry/consumer_delivery_integration_test.go |
| heartbeat-consumption | Each consumer applies the event once | TestTwoConsumersEachApplyTheEventOnce | internal/telemetry/consumer_delivery_integration_test.go |
| heartbeat-event-publication | Redelivered heartbeats are published with their original identity | TestPublisherRepublishesRedeliveredHeartbeatsWithTheirIdentity | internal/telemetry/publisher_integration_test.go |
| message-broker-topology | A consumer narrows to one kind of notification | TestTopologyNarrowedNotificationBinding | internal/telemetry/topology_routing_integration_test.go |
| message-broker-topology | Declaration precedes use | TestTopologyDeclaredBeforeFirstUse | internal/telemetry/topology_restart_integration_test.go |
| message-broker-topology | Every event family has its own key space | TestTopologyEventFamiliesHaveDisjointKeySpaces | internal/telemetry/topology_routing_integration_test.go |
| message-broker-topology | Layout survives a broker restart | TestTopologySurvivesBrokerRestartWithWaitingMessages | internal/telemetry/topology_restart_integration_test.go |
| rollback-notification | A rejected publication is retried | TestNotifierRetriesARejectedPublication | internal/telemetry/notifier_test.go |
| rollback-notification | A rollout that could not start announces nothing | TestRolloutThatCannotStartRunsNoCompensation | internal/temporal/rollback_saga_coverage_test.go |
| telemetry-ingest | Heartbeats are queryable by device and time range | TestWriterHeartbeatsAreQueryableByDeviceAndTimeRange | internal/telemetry/writer_ingest_integration_test.go |
| telemetry-ingest | Redelivery after a control-plane restart | TestWriterRedeliveryAfterRestartStoresOneDocumentPerEvent | internal/telemetry/writer_ingest_integration_test.go |
| device-downgrade | A resolved failure is recorded for the device alone | TestDowngradeRecordsAResolvedFailureForTheDeviceAlone | internal/temporal/rollback_deviceworld_test.go |
| firmware-inventory | An unreadable device is never assumed correct | TestReconcileInventoryLeavesAnUnreadableDeviceUnverified | internal/temporal/rollback_deviceworld_test.go |
| firmware-registry | Resolving a version reads metadata, not payloads | TestMetadataByVersion | internal/firmware/store_test.go |
| rollback-saga | A rollout that could not start runs no compensation | TestRolloutThatCannotStartRunsNoCompensation | internal/temporal/rollback_saga_coverage_test.go |
| rollback-saga | Re-running a finished plan changes nothing | TestRerunningAFinishedPlanChangesNothing | internal/temporal/rollback_saga_coverage_test.go |
| rollout-control | A pause during the dispatch wait is applied | TestRolloutWorkflowPauseDuringTheDispatchWaitIsApplied | internal/temporal/rollout_pause_coverage_test.go |
| rollout-workflow | A paused rollout reports the pause | TestRolloutWorkflowPausedQueryReportsThePause | internal/temporal/rollout_pause_coverage_test.go |
| rollout-workflow | Membership is recorded before anything is dispatched | TestRolloutWorkflowRecordsMembershipBeforeDispatch | internal/temporal/rollout_membership_test.go |
| rollout-api | A repeated start is refused | TestHandlerStartRefusesASecondExecution | internal/rolloutapi/start_refusal_test.go |
| rollout-api | A rollout that cannot start is still accepted | TestHandlerStartAcceptsARolloutThatCannotStart | internal/rolloutapi/cannot_start_test.go |
| rollout-search-attributes | Filtering rollouts in the Temporal UI | TestRolloutVisibilityFiltersRolloutRuns | internal/rolloutapi/visibility_integration_test.go |
| mongo-data-model | Dedup lookup is served by an index | TestDedupLookupIsServedByAnIndex | internal/telemetry/dedup_index_integration_test.go |
| mongo-data-model | Device identity lookup | TestDeviceIdentityLookup | internal/devices/identity_integration_test.go |
| mongo-data-model | Domain records never expire | TestDomainRecordsNeverExpire | internal/mongotest/retention_integration_test.go |
| mongo-data-model | Metadata never holds payloads | TestMetadataNeverHoldsPayload | internal/firmware/metadata_integration_test.go |
| mongo-data-model | Retention is bounded and configurable in one place | TestLedgerRetentionIsBoundedAndConfigurable | internal/mongotest/retention_integration_test.go |
| mongo-data-model | Status queries | TestStatusQueriesUseStatusIndexes | internal/mongotest/index_usage_integration_test.go |
| mongo-data-model | Waves roll up to their rollout | TestWavesRollUpToTheirRollout | internal/rollout/waves_rollup_integration_test.go |
| device-search-attributes | Filtering devices in the Temporal UI | TestDeviceVisibilityFiltersDeviceRuns | internal/temporal/device_visibility_integration_test.go |

## Exemptions

Each exemption below falls into one of three categories. **STACK-DEPENDENT**: the scenario's verification needs a running local stack - a live worker binary, several replicas, or the Temporal UI - that the delivery plan has not reached, so the behavior is implemented but unverifiable in this delivery's tests. **RECORDED-NOT-IMPLEMENTED**: the specified behavior this delivery deliberately does not implement, recorded as out of scope in `openspec/changes/add-missing-feature-coverage/proposal.md` under "Recorded but not implemented here"; the exemption is deleted when the owning change implements the behavior and its test. **MEASURED-RUN**: the scenario is a statement about what a whole suite run costs - its wall clock, peak memory, or absence of resource failures - which no Go test can assert about a run that contains it; it is verified by the recorded run the change's tasks name, and the exemption states which run and what it measured.

| Capability | Scenario | Reason | What would verify it |
| --- | --- | --- | --- |
| temporal-worker | Two replicas share the load | STACK-DEPENDENT: Needs a running local stack: two live worker replicas polling the same task queue while device workflows run, which this delivery's tests cannot start (they are unit and in-process workflow tests only). Backed behavior, deferred verification. | A worker integration test that starts two replicas against a shared Temporal server, exercises device workflows, and asserts every task ran once and the observable device state matches a single-replica run (for example a cmd/worker test running two replicas). |
| temporal-worker | Replica loss changes nothing observable | STACK-DEPENDENT: Needs a running local stack: several live worker replicas, one of which is stopped while workflows are active, so the surviving replicas' continuation cannot be reproduced in-process. | A worker integration test that starts two replicas, kills one mid-execution, and asserts the remaining replica keeps the device workflows running with no snapshot or state change lost. |
| temporal-worker | Graceful shutdown | STACK-DEPENDENT: The live-drain half, SIGTERM arriving while tasks execute and in-flight work then finishing inside the shutdown timeout, needs a live worker binary and a running Temporal server; no in-process test can produce the signal against executing tasks. | A worker integration test that starts the worker binary, sends SIGTERM while it executes tasks, and asserts it stops polling, drains in-flight work within the shutdown timeout, and exits without error. |
| repo-scaffold | Stack lifecycle targets | RECORDED-NOT-IMPLEMENTED: Half the scenario is backed — TestStackLifecycleTargetsPointAtDeploy (internal/repolayout/makefile_test.go) asserts make up and make down run kubectl apply/delete -f deploy/ and do not swallow the tooling's exit status. The other half cannot be verified because no manifest exists to bring up: proposal.md's "Explicitly not in this change" excludes "compose/k3d manifests or stack bring-up", so the deploy/ tree holds notes and no stack definition. | The deploy/ manifests landing with the deployment stage, and a test that runs make up against them and asserts the stack's resources exist and make down removes them; the wiring half is already asserted and fails if make up stops naming deploy/ or starts swallowing kubectl's failure. |
| mongo-data-model | Retention is configurable in one place | RECORDED-NOT-IMPLEMENTED: The behavior is not implemented in this delivery: the TTL index is built from FLEETOPS_TELEMETRY_RETENTION_DAYS in deploy/mongo/init.js while internal/config has no retention field, so the ingestion path does not read the same value. proposal.md § "Recorded but not implemented here" records the telemetry retention not being one named value as out of scope. | A test that changes the one named retention value, re-runs the bootstrap, asserts the index expireAfterSeconds follows it, and asserts the ingestion path reads the same configuration value. |
| mongo-data-model | Telemetry document content | RECORDED-NOT-IMPLEMENTED: The behavior is not implemented in this delivery: telemetry is a plain MongoDB collection, not the time-series collection the requirement specifies, a deliberate deviation proposal.md § "Recorded but not implemented here" records (the time-series wording divergence) and leaves to the change that owns the capability. | A test asserting the collection is created with time-series options (listCollections type timeseries, timeField ts) and that an ingested heartbeat is retrievable from it by device and time range. |
| test-execution-profiles | The weak-host profile runs the suite within its budget | MEASURED-RUN: No Go test can assert what a suite run costs: the peak is dominated by container processes outside the test binary, and a test asserting the memory of a workload it constructed itself would prove nothing about the suite. The configuration half is backed — TestWeakProfileFitsTheDocumentedBudget (internal/testexec/narrowing_test.go) asserts the weak profile's fleet size, wait multiplier, startup budgets and container sharing stay within the documented envelope and narrow every knob without touching an assertion. | The recorded weak-host run: `make test-weak` (`FLEETOPS_TEST_PROFILE=weak go test -tags integration -count=1 -timeout 30m ./...`) with its wall clock and peak container memory sampled during the run and recorded in tasks.md § 8.2, on a machine with at most 4 GB of RAM and 4 CPUs, exiting green with no resource-reason failure. |
