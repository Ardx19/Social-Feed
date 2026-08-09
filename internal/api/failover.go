package api

import "log"

// failover.go wires the health monitor and write queue into the coordinator's
// read/write paths (U10). See design.md for the resilience contract.
// readAddrFor returns the nodeID and the addr to READ from for an entity: the
// primary when it is up, its replica when the primary is marked down. Ring
// membership is static — failover never mutates the ring.
func (c *CoordinatorServer) readAddrFor(entityID string) (nodeID, addr string, err error) {
	nodeID, primary, err := c.ShardFor(entityID)
	if err != nil {
		return "", "", err
	}
	if c.monitor != nil && c.monitor.IsDown(nodeID) {
		if rep, ok := c.monitor.ReplicaFor(nodeID); ok {
			return nodeID, rep, nil
		}
	}
	return nodeID, primary, nil
}

// readWithFailover reads a key from the entity's primary, retrying once via
// the replica when the primary errors (the undetected-death window: the
// primary is down but the heartbeat hasn't marked it yet). Returns the value
// and whether it was found.
func (c *CoordinatorServer) readWithFailover(nodeID, addr, key string) ([]byte, bool, error) {
	val, found, err := c.client.Read(addr, key)
	if err != nil && c.monitor != nil {
		if rep, ok := c.monitor.ReplicaFor(nodeID); ok {
			rval, rfound, rerr := c.client.Read(rep, key)
			if rerr == nil {
				return rval, rfound, nil
			}
		}
	}
	return val, found, err
}

// writeToShard writes to a node's primary, queueing the op instead when the
// primary is marked down. Returns:
//   - (true, nil)    if the op was queued (accepted, will replay on recovery)
//   - (false, nil)   if it was written directly
//   - (false, ErrQueueFull) if it could not be queued (caller -> 503)
func (c *CoordinatorServer) writeToShard(nodeID, addr string, op QueuedOp, do func() error) (bool, error) {
	if c.monitor != nil && c.monitor.IsDown(nodeID) {
		if err := c.queue.Enqueue(op); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, do()
}

// drainQueue replays queued writes to a recovered primary, in order.
// Idempotent by construction (absolute values).
func (c *CoordinatorServer) drainQueue(nodeID string) {
	addr := c.monitor.PrimaryAddr(nodeID)
	for _, op := range c.queue.Drain(addr) {
		if len(op.Pairs) > 0 {
			if err := c.client.Write(addr, op.Pairs); err != nil {
				logWrite(nodeID, op, err)
				continue
			}
		}
		for _, d := range op.Deletes {
			if err := c.client.Delete(addr, string(d)); err != nil {
				logWrite(nodeID, op, err)
				break
			}
		}
	}
}

func logWrite(nodeID string, op QueuedOp, err error) {
	log.Printf("drain: replay to %s failed: %v (queued write lost — accepted gap)", nodeID, err)
}
