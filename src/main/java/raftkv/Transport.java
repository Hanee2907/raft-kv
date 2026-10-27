package raftkv;

import java.util.function.BiConsumer;
import raftkv.Messages.Message;

/**
 * The only thing Raft needs from the network: best-effort, asynchronous, unordered message delivery
 * to a peer by id. Messages may be dropped, delayed, duplicated or reordered; Raft tolerates all of it.
 *
 * <p>Implementations: {@code TcpTransport} (real sockets) and the test suite's {@code SimNetwork}
 * (in-process, with partitions). A Netty or gRPC transport would implement this same interface.
 */
public interface Transport {

    /** Never blocks for long and never throws; silently drops the message if the peer is unreachable. */
    void send(String to, Message message);

    /** Registers the handler for inbound messages, called as {@code receiver.accept(senderId, message)}. Null detaches. */
    void setReceiver(BiConsumer<String, Message> receiver);
}
