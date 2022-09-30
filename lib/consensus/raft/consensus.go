// Package raft implements a Consensus component for IPFS Cluster which uses
// Raft (go-libp2p-raft).
package consensus

import (
	"context"
	"errors"
	"fmt"
	"github.com/filecoin-project/lotus/lib/addrutil"
	"sort"
	"time"

	"github.com/google/uuid"
	"golang.org/x/exp/slices"

	addr "github.com/filecoin-project/go-address"

	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/messagepool"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/node/repo"

	//ds "github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	consensus "github.com/libp2p/go-libp2p-consensus"
	rpc "github.com/libp2p/go-libp2p-gorpc"
	libp2praft "github.com/libp2p/go-libp2p-raft"
	host "github.com/libp2p/go-libp2p/core/host"
	peer "github.com/libp2p/go-libp2p/core/peer"
)

var logger = logging.Logger("raft")

//type NonceMapType map[addr.Address]uint64
//type MsgUuidMapType map[uuid.UUID]*types.SignedMessage

type RaftState struct {
	NonceMap api.NonceMapType
	MsgUuids api.MsgUuidMapType

	// TODO: add comment explaining why this is needed
	// We need a reference to the messagepool in the raft state in order to
	// sync messages that have been sent by the leader node
	// Miner calls StateWaitMsg after MpoolPushMessage to check if the message has
	// landed on chain. This check requires the message be stored in the local chainstore
	// If a leadernode goes down after sending a message to the chain and is replaced by
	// another node, the other node needs to have this message in its chainstore for the
	// above check to succeed.

	// This is because the miner only stores signed CIDs but the message received from in a
	// block will be unsigned (for BLS). Hence, the process relies on the node to store the
	// signed message which holds a copy of the unsigned message to properly perform all the
	// needed checks
	Mpool *messagepool.MessagePool
}

func newRaftState(mpool *messagepool.MessagePool) *RaftState {
	return &RaftState{
		NonceMap: make(map[addr.Address]uint64),
		MsgUuids: make(map[uuid.UUID]*types.SignedMessage),
		Mpool:    mpool,
	}
}

type ConsensusOp struct {
	Nonce     uint64               `codec:"nonce,omitempty"`
	Uuid      uuid.UUID            `codec:"uuid,omitempty"`
	Addr      addr.Address         `codec:"addr,omitempty"`
	SignedMsg *types.SignedMessage `codec:"signedMsg,omitempty"`
}

func (c ConsensusOp) ApplyTo(state consensus.State) (consensus.State, error) {
	s := state.(*RaftState)
	s.NonceMap[c.Addr] = c.Nonce
	s.MsgUuids[c.Uuid] = c.SignedMsg
	s.Mpool.Add(context.TODO(), c.SignedMsg)
	return s, nil
}

var _ consensus.Op = &ConsensusOp{}

// Consensus handles the work of keeping a shared-state between
// the peers of an IPFS Cluster, as well as modifying that state and
// applying any updates in a thread-safe manner.
type Consensus struct {
	ctx    context.Context
	cancel func()
	config *ClusterRaftConfig

	host host.Host

	consensus consensus.OpLogConsensus
	actor     consensus.Actor
	raft      *raftWrapper
	state     *RaftState

	rpcClient *rpc.Client
	rpcReady  chan struct{}
	readyCh   chan struct{}

	peerSet []peer.ID
	repo    repo.LockedRepo

	//shutdownLock sync.RWMutex
	//shutdown     bool
}

// NewConsensus builds a new ClusterConsensus component using Raft.
//
// Raft saves state snapshots regularly and persists log data in a bolt
// datastore. Therefore, unless memory usage is a concern, it is recommended
// to use an in-memory go-datastore as store parameter.
//
// The staging parameter controls if the Raft peer should start in
// staging mode (used when joining a new Raft peerset with other peers).
func NewConsensus(host host.Host, cfg *ClusterRaftConfig, mpool *messagepool.MessagePool, repo repo.LockedRepo, staging bool) (*Consensus, error) {
	err := ValidateConfig(cfg)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	logger.Debug("starting Consensus and waiting for a leader...")
	state := newRaftState(mpool)

	consensus := libp2praft.NewOpLog(state, &ConsensusOp{})

	raft, err := newRaftWrapper(host, cfg, consensus.FSM(), repo, staging)
	if err != nil {
		logger.Error("error creating raft: ", err)
		cancel()
		return nil, err
	}
	actor := libp2praft.NewActor(raft.raft)
	consensus.SetActor(actor)

	peers := []peer.ID{}
	addrInfos, err := addrutil.ParseAddresses(ctx, cfg.InitPeerset)
	for _, addrInfo := range addrInfos {
		peers = append(peers, addrInfo.ID)

		// Add peer to address book
		host.Peerstore().AddAddrs(addrInfo.ID, addrInfo.Addrs, time.Duration(time.Hour*100))
	}

	cc := &Consensus{
		ctx:       ctx,
		cancel:    cancel,
		config:    cfg,
		host:      host,
		consensus: consensus,
		actor:     actor,
		state:     state,
		raft:      raft,
		peerSet:   peers,
		rpcReady:  make(chan struct{}, 1),
		readyCh:   make(chan struct{}, 1),
		repo:      repo,
	}

	go cc.finishBootstrap()
	return cc, nil

}

func NewConsensusWithRPCClient(staging bool) func(host host.Host,
	cfg *ClusterRaftConfig,
	rpcClient *rpc.Client,
	mpool *messagepool.MessagePool,
	repo repo.LockedRepo,
) (*Consensus, error) {

	return func(host host.Host, cfg *ClusterRaftConfig, rpcClient *rpc.Client, mpool *messagepool.MessagePool, repo repo.LockedRepo) (*Consensus, error) {
		cc, err := NewConsensus(host, cfg, mpool, repo, staging)
		if err != nil {
			return nil, err
		}
		cc.rpcClient = rpcClient
		cc.rpcReady <- struct{}{}
		return cc, nil
	}
}

// WaitForSync waits for a leader and for the state to be up to date, then returns.
func (cc *Consensus) WaitForSync(ctx context.Context) error {
	//ctx, span := trace.StartSpan(ctx, "consensus/WaitForSync")
	//defer span.End()

	leaderCtx, cancel := context.WithTimeout(
		ctx,
		time.Duration(cc.config.WaitForLeaderTimeout))
	defer cancel()

	// 1 - wait for leader
	// 2 - wait until we are a Voter
	// 3 - wait until last index is applied

	// From raft docs:

	// once a staging server receives enough log entries to be sufficiently
	// caught up to the leader's log, the leader will invoke a  membership
	// change to change the Staging server to a Voter

	// Thus, waiting to be a Voter is a guarantee that we have a reasonable
	// up to date state. Otherwise, we might return too early (see
	// https://github.com/ipfs-cluster/ipfs-cluster/issues/378)

	_, err := cc.raft.WaitForLeader(leaderCtx)
	if err != nil {
		return errors.New("error waiting for leader: " + err.Error())
	}

	err = cc.raft.WaitForVoter(ctx)
	if err != nil {
		return errors.New("error waiting to become a Voter: " + err.Error())
	}

	err = cc.raft.WaitForUpdates(ctx)
	if err != nil {
		return errors.New("error waiting for consensus updates: " + err.Error())
	}
	return nil
}

// waits until there is a consensus leader and syncs the state
// to the tracker. If errors happen, this will return and never
// signal the component as Ready.
func (cc *Consensus) finishBootstrap() {
	// wait until we have RPC to perform any actions.
	select {
	case <-cc.ctx.Done():
		return
	case <-cc.rpcReady:
	}

	// Sometimes bootstrap is a no-Op. It only applies when
	// no state exists and staging=false.
	_, err := cc.raft.Bootstrap()
	if err != nil {
		return
	}

	logger.Debugf("Bootstrap finished")
	err = cc.WaitForSync(cc.ctx)
	if err != nil {
		return
	}
	logger.Debug("Raft state is now up to date")
	logger.Debug("consensus ready")
	cc.readyCh <- struct{}{}
}

// Shutdown stops the component so it will not process any
// more updates. The underlying consensus is permanently
// shutdown, along with the libp2p transport.
func (cc *Consensus) Shutdown(ctx context.Context) error {
	//ctx, span := trace.StartSpan(ctx, "consensus/Shutdown")
	//defer span.End()

	//cc.shutdownLock.Lock()
	//defer cc.shutdownLock.Unlock()

	//if cc.shutdown {
	//	logger.Debug("already shutdown")
	//	return nil
	//}

	logger.Info("stopping Consensus component")

	// Raft Shutdown
	err := cc.raft.Shutdown(ctx)
	if err != nil {
		logger.Error(err)
	}

	if cc.config.HostShutdown {
		cc.host.Close()
	}

	//cc.shutdown = true
	cc.cancel()
	close(cc.rpcReady)
	return nil
}

// Ready returns a channel which is signaled when the Consensus
// algorithm has finished bootstrapping and is ready to use
func (cc *Consensus) Ready(ctx context.Context) <-chan struct{} {
	//_, span := trace.StartSpan(ctx, "consensus/Ready")
	//defer span.End()

	return cc.readyCh
}

// IsTrustedPeer returns true. In Raft we trust all peers.
func (cc *Consensus) IsTrustedPeer(ctx context.Context, p peer.ID) bool {
	return slices.Contains(cc.peerSet, p)
}

// Trust is a no-Op.
func (cc *Consensus) Trust(ctx context.Context, pid peer.ID) error { return nil }

// Distrust is a no-Op.
func (cc *Consensus) Distrust(ctx context.Context, pid peer.ID) error { return nil }

// returns true if the operation was redirected to the leader
// note that if the leader just dissappeared, the rpc call will
// fail because we haven't heard that it's gone.
func (cc *Consensus) RedirectToLeader(method string, arg interface{}, ret interface{}) (bool, error) {
	//ctx, span := trace.StartSpan(cc.ctx, "consensus/RedirectToLeader")
	//defer span.End()
	ctx := cc.ctx

	var finalErr error

	// Retry redirects
	for i := 0; i <= cc.config.CommitRetries; i++ {
		logger.Debugf("redirect try %d", i)
		leader, err := cc.Leader(ctx)

		// No leader, wait for one
		if err != nil {
			logger.Warn("there seems to be no leader. Waiting for one")
			rctx, cancel := context.WithTimeout(
				ctx,
				time.Duration(cc.config.WaitForLeaderTimeout),
			)
			defer cancel()
			pidstr, err := cc.raft.WaitForLeader(rctx)

			// means we timed out waiting for a leader
			// we don't retry in this case
			if err != nil {
				return false, fmt.Errorf("timed out waiting for leader: %s", err)
			}
			leader, err = peer.Decode(pidstr)
			if err != nil {
				return false, err
			}
		}

		logger.Infof("leader: %s, curr host: %s, peerSet: %s", leader, cc.host.ID(), cc.peerSet)

		// We are the leader. Do not redirect
		if leader == cc.host.ID() {
			return false, nil
		}

		logger.Debugf("redirecting %s to leader: %s", method, leader.Pretty())
		finalErr = cc.rpcClient.CallContext(
			ctx,
			leader,
			"Consensus",
			method,
			arg,
			ret,
		)
		if finalErr != nil {
			logger.Errorf("retrying to redirect request to leader: %s", finalErr)
			time.Sleep(2 * cc.config.RaftConfig.HeartbeatTimeout)
			continue
		}
		break
	}

	// We tried to redirect, but something happened
	return true, finalErr
}

// commit submits a cc.consensus commit. It retries upon failures.
func (cc *Consensus) Commit(ctx context.Context, op *ConsensusOp) error {
	//ctx, span := trace.StartSpan(ctx, "consensus/commit")
	//defer span.End()
	//
	//if cc.config.Tracing {
	//	// required to cross the serialized boundary
	//	Op.SpanCtx = span.SpanContext()
	//	tagmap := tag.FromContext(ctx)
	//	if tagmap != nil {
	//		Op.TagCtx = tag.Encode(tagmap)
	//	}
	//}

	var finalErr error
	for i := 0; i <= cc.config.CommitRetries; i++ {
		logger.Debugf("attempt #%d: committing %+v", i, op)

		// this means we are retrying
		if finalErr != nil {
			logger.Errorf("retrying upon failed commit (retry %d): %s ",
				i, finalErr)
		}

		// try to send it to the leader
		// RedirectToLeader has it's own retry loop. If this fails
		// we're done here.

		//ok, err := cc.RedirectToLeader(rpcOp, redirectArg, struct{}{})
		//if err != nil || ok {
		//	return err
		//}

		// Being here means we are the LEADER. We can commit.

		// now commit the changes to our state
		//cc.shutdownLock.RLock() // do not shut down while committing
		_, finalErr = cc.consensus.CommitOp(op)
		//cc.shutdownLock.RUnlock()
		if finalErr != nil {
			goto RETRY
		}

	RETRY:
		time.Sleep(time.Duration(cc.config.CommitRetryDelay))
	}
	return finalErr
}

// AddPeer adds a new peer to participate in this consensus. It will
// forward the operation to the leader if this is not it.
func (cc *Consensus) AddPeer(ctx context.Context, pid peer.ID) error {
	//ctx, span := trace.StartSpan(ctx, "consensus/AddPeer")
	//defer span.End()

	var finalErr error
	for i := 0; i <= cc.config.CommitRetries; i++ {
		logger.Debugf("attempt #%d: AddPeer %s", i, pid.Pretty())
		if finalErr != nil {
			logger.Errorf("retrying to add peer. Attempt #%d failed: %s", i, finalErr)
		}
		ok, err := cc.RedirectToLeader("AddPeer", pid, struct{}{})
		if err != nil || ok {
			return err
		}
		// Being here means we are the leader and can commit
		//cc.shutdownLock.RLock() // do not shutdown while committing
		//finalErr = cc.raft.AddPeer(ctx, peer.Encode(pid))
		finalErr = cc.raft.AddPeer(ctx, pid)

		//cc.shutdownLock.RUnlock()
		if finalErr != nil {
			time.Sleep(time.Duration(cc.config.CommitRetryDelay))
			continue
		}
		logger.Infof("peer added to Raft: %s", pid.Pretty())
		break
	}
	return finalErr
}

// RmPeer removes a peer from this consensus. It will
// forward the operation to the leader if this is not it.
func (cc *Consensus) RmPeer(ctx context.Context, pid peer.ID) error {
	//ctx, span := trace.StartSpan(ctx, "consensus/RmPeer")
	//defer span.End()

	var finalErr error
	for i := 0; i <= cc.config.CommitRetries; i++ {
		logger.Debugf("attempt #%d: RmPeer %s", i, pid.Pretty())
		if finalErr != nil {
			logger.Errorf("retrying to remove peer. Attempt #%d failed: %s", i, finalErr)
		}
		ok, err := cc.RedirectToLeader("RmPeer", pid, struct{}{})
		if err != nil || ok {
			return err
		}
		// Being here means we are the leader and can commit
		//cc.shutdownLock.RLock() // do not shutdown while committing
		finalErr = cc.raft.RemovePeer(ctx, peer.Encode(pid))
		//cc.shutdownLock.RUnlock()
		if finalErr != nil {
			time.Sleep(time.Duration(cc.config.CommitRetryDelay))
			continue
		}
		logger.Infof("peer removed from Raft: %s", pid.Pretty())
		break
	}
	return finalErr
}

// RaftState retrieves the current consensus RaftState. It may error if no RaftState has
// been agreed upon or the state is not consistent. The returned RaftState is the
// last agreed-upon RaftState known by this node. No writes are allowed, as all
// writes to the shared state should happen through the Consensus component
// methods.
func (cc *Consensus) State(ctx context.Context) (*RaftState, error) {
	//_, span := trace.StartSpan(ctx, "consensus/RaftState")
	//defer span.End()

	st, err := cc.consensus.GetLogHead()
	if err == libp2praft.ErrNoState {
		return newRaftState(nil), nil
	}

	if err != nil {
		return nil, err
	}
	state, ok := st.(*RaftState)
	if !ok {
		return nil, errors.New("wrong state type")
	}
	return state, nil
}

// Leader returns the peerID of the Leader of the
// cluster. It returns an error when there is no leader.
func (cc *Consensus) Leader(ctx context.Context) (peer.ID, error) {
	//_, span := trace.StartSpan(ctx, "consensus/Leader")
	//defer span.End()

	// Note the hard-dependency on raft here...
	raftactor := cc.actor.(*libp2praft.Actor)
	return raftactor.Leader()
}

// Clean removes the Raft persisted state.
func (cc *Consensus) Clean(ctx context.Context) error {
	//_, span := trace.StartSpan(ctx, "consensus/Clean")
	//defer span.End()

	//cc.shutdownLock.RLock()
	//defer cc.shutdownLock.RUnlock()
	//if !cc.shutdown {
	//	return errors.New("consensus component is not shutdown")
	//}

	//return CleanupRaft(cc.config)
	return nil
}

//Rollback replaces the current agreed-upon
//state with the state provided. Only the consensus leader
//can perform this operation.
//func (cc *Consensus) Rollback(state RaftState) error {
//	// This is unused. It *might* be used for upgrades.
//	// There is rather untested magic in libp2p-raft's FSM()
//	// to make this possible.
//	return cc.consensus.Rollback(state)
//}

// Peers return the current list of peers in the consensus.
// The list will be sorted alphabetically.
func (cc *Consensus) Peers(ctx context.Context) ([]peer.ID, error) {
	//ctx, span := trace.StartSpan(ctx, "consensus/Peers")
	//defer span.End()

	//cc.shutdownLock.RLock() // prevent shutdown while here
	//defer cc.shutdownLock.RUnlock()
	//
	//if cc.shutdown { // things hang a lot in this case
	//	return nil, errors.New("consensus is shutdown")
	//}
	peers := []peer.ID{}
	raftPeers, err := cc.raft.Peers(ctx)
	if err != nil {
		return nil, fmt.Errorf("cannot retrieve list of peers: %s", err)
	}

	sort.Strings(raftPeers)

	for _, p := range raftPeers {
		id, err := peer.Decode(p)
		if err != nil {
			panic("could not decode peer")
		}
		peers = append(peers, id)
	}
	return peers, nil
}

func (cc *Consensus) IsLeader(ctx context.Context) bool {
	leader, _ := cc.Leader(ctx)
	return leader == cc.host.ID()
}

// OfflineState state returns a cluster state by reading the Raft data and
// writing it to the given datastore which is then wrapped as a state.RaftState.
// Usually an in-memory datastore suffices. The given datastore should be
// thread-safe.
//func OfflineState(cfg *Config, store ds.Datastore) (state.RaftState, error) {
//	r, snapExists, err := LastStateRaw(cfg)
//	if err != nil {
//		return nil, err
//	}
//
//	st, err := dsstate.New(context.Background(), store, cfg.DatastoreNamespace, dsstate.DefaultHandle())
//	if err != nil {
//		return nil, err
//	}
//	if !snapExists {
//		return st, nil
//	}
//
//	err = st.Unmarshal(r)
//	if err != nil {
//		return nil, err
//	}
//	return st, nil
//}
