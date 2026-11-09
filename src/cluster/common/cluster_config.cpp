#include "cluster/common/cluster_config.h"

namespace deepmsg::cluster::common
{

axc::core::NameTable cluster_name_table()
{
    axc::core::NameTable table;

    table.add_accepted_ignored(
        "aeron.cluster.accept.standby.snapshots",
        "premium standby produces them; this implementation keeps the shell and does not replicate");
    table.add_honoured("aeron.cluster.appointed.leader.id");
    table.add_honoured("aeron.cluster.authenticator.supplier");
    table.add_honoured("aeron.cluster.authorisation.service.supplier");
    table.add_honoured("aeron.cluster.backup.catchup.channel");
    table.add_honoured("aeron.cluster.backup.catchup.endpoint");
    table.add_honoured("aeron.cluster.backup.cool.down.interval");
    table.add_honoured("aeron.cluster.backup.interval");
    table.add_honoured("aeron.cluster.backup.progress.timeout");
    table.add_honoured("aeron.cluster.backup.response.timeout");
    table.add_honoured("aeron.cluster.backup.source.type");
    table.add_honoured("aeron.cluster.client.name");
    table.add_honoured("aeron.cluster.clock");
    table.add_honoured("aeron.cluster.consensus.channel");
    table.add_honoured("aeron.cluster.consensus.control.enable");
    table.add_honoured("aeron.cluster.consensus.endpoints");
    table.add_honoured("aeron.cluster.consensus.module.agent.role.name");
    table.add_accepted_ignored(
        "aeron.cluster.consensus.module.extension",
        "optional extension; no implementation, external schema messages are handed through as in the reference");
    table.add_honoured("aeron.cluster.consensus.module.stream.id");
    table.add_honoured("aeron.cluster.consensus.stream.id");
    table.add_honoured("aeron.cluster.control.channel");
    table.add_honoured("aeron.cluster.cpu.affinity");
    table.add_honoured("aeron.cluster.cycle.threshold");
    table.add_honoured("aeron.cluster.dir");
    table.add_honoured("aeron.cluster.egress.channel");
    table.add_honoured("aeron.cluster.egress.stream.id");
    table.add_honoured("aeron.cluster.election.status.interval");
    table.add_honoured("aeron.cluster.election.timeout");
    table.add_honoured("aeron.cluster.error.buffer.length");
    table.add_honoured("aeron.cluster.file.sync.level");
    table.add_honoured("aeron.cluster.follower.catchup.channel");
    table.add_honoured("aeron.cluster.id");
    table.add_honoured("aeron.cluster.idle.strategy");
    table.add_honoured("aeron.cluster.ingress.channel");
    table.add_honoured("aeron.cluster.ingress.endpoints");
    table.add_honoured("aeron.cluster.ingress.fragment.limit");
    table.add_honoured("aeron.cluster.ingress.ipc.allowed");
    table.add_honoured("aeron.cluster.ingress.stream.id");
    table.add_honoured("aeron.cluster.leader.archive.control.channel");
    table.add_honoured("aeron.cluster.leader.heartbeat.interval");
    table.add_honoured("aeron.cluster.leader.heartbeat.timeout");
    table.add_honoured("aeron.cluster.log.channel");
    table.add_honoured("aeron.cluster.log.control.enable");
    table.add_honoured("aeron.cluster.log.fragment.limit");
    table.add_honoured("aeron.cluster.log.stream.id");
    table.add_honoured("aeron.cluster.mark.file.dir");
    table.add_honoured("aeron.cluster.max.sessions");
    table.add_honoured("aeron.cluster.member.endpoints");
    table.add_honoured("aeron.cluster.member.id");
    table.add_honoured("aeron.cluster.members");
    table.add_honoured("aeron.cluster.message.timeout");
    table.add_honoured("aeron.cluster.replay.channel");
    table.add_honoured("aeron.cluster.replay.stream.id");
    table.add_honoured("aeron.cluster.replication.channel");
    table.add_honoured("aeron.cluster.replication.progress.interval");
    table.add_honoured("aeron.cluster.replication.progress.timeout");
    table.add_honoured("aeron.cluster.service.class.name");
    table.add_honoured("aeron.cluster.service.count");
    table.add_honoured("aeron.cluster.service.cpu.affinity");
    table.add_honoured("aeron.cluster.service.cycle.threshold");
    table.add_honoured("aeron.cluster.service.delegating.error.handler");
    table.add_honoured("aeron.cluster.service.error.buffer.length");
    table.add_honoured("aeron.cluster.service.id");
    table.add_honoured("aeron.cluster.service.name");
    table.add_honoured("aeron.cluster.service.responder");
    table.add_honoured("aeron.cluster.service.snapshot.threshold");
    table.add_honoured("aeron.cluster.service.stream.id");
    table.add_honoured("aeron.cluster.services.dir");
    table.add_honoured("aeron.cluster.session.timeout");
    table.add_honoured("aeron.cluster.snapshot.channel");
    table.add_honoured("aeron.cluster.snapshot.stream.id");
    table.add_accepted_ignored("aeron.cluster.standby.snapshot.notification.processing.delay",
                               "premium standby path; parsed but does not enter the service");
    table.add_honoured("aeron.cluster.startup.canvass.timeout");
    table.add_honoured("aeron.cluster.termination.timeout");
    table.add_honoured("aeron.cluster.ticks.per.wheel");
    table.add_honoured("aeron.cluster.timer.service.supplier");
    table.add_honoured("aeron.cluster.tool.delay");
    table.add_honoured("aeron.cluster.tool.replay.channel");
    table.add_honoured("aeron.cluster.tool.replay.stream.id");
    table.add_honoured("aeron.cluster.tool.timeout");
    table.add_honoured("aeron.cluster.total.snapshot.threshold");
    table.add_honoured("aeron.cluster.wheel.tick.resolution");

    return table;
}

}  // namespace deepmsg::cluster::common
