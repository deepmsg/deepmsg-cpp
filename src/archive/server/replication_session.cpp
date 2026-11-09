#include "archive/server/replication_session.h"

#include <utility>

namespace deepmsg::archive::server
{

ReplicationSession::ReplicationSession(const ReplicationConfig& config) : config_(config) {}

int ReplicationSession::do_work()
{
    // `ReplicationSession.doWork` (`:273-280`): one state per pass. The state
    // bodies arrive in A4b-4..A4b-7; until CONNECT is wired the session simply
    // stays alive, which is the reference's behaviour before it connects too.
    return 0;
}

void ReplicationSession::abort(std::string_view reason)
{
    (void)reason;
    // `:200-215`: the stop path only ends the session; `close()` does the
    // teardown and sends `REPLICATE_END`.
    state_ = State::done;
    is_done_ = true;
}

void ReplicationSession::close()
{
    // `:196-213`: stop the destination subscription and the source replay and
    // send `REPLICATE_END`. Those land with the states that own them.
}

}  // namespace deepmsg::archive::server
