package experiments

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/shinzonetwork/app-sdk/pkg/attestation"
	"github.com/shinzonetwork/app-sdk/pkg/defra"
	"github.com/shinzonetwork/app-sdk/pkg/logger"
	"github.com/sourcenetwork/defradb/acp/identity"
	"github.com/sourcenetwork/defradb/crypto"
	"github.com/sourcenetwork/defradb/http"
	netConfig "github.com/sourcenetwork/defradb/net/config"
	"github.com/sourcenetwork/defradb/node"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	logger.Init(true)
	exitCode := m.Run()
	os.Exit(exitCode)
}

// This test shows us a simple example of P2P active replication
// We spinup two separate defra instances, one who writes data, the other who attempts to read the data
// You'll notice that the reader defra instance is not able to read the data until it is set as a replicator
func TestSimpleP2PReplication(t *testing.T) {
	listenAddress := "/ip4/127.0.0.1/tcp/0"
	defraUrl := "127.0.0.1:0"
	options := []node.Option{
		node.WithDisableAPI(false),
		node.WithDisableP2P(false),
		node.WithStorePath(t.TempDir()),
		http.WithAddress(defraUrl),
		netConfig.WithListenAddresses(listenAddress),
	}
	ctx := context.Background()
	writerDefra := StartDefraInstance(t, ctx, options)
	defer writerDefra.Close(ctx)

	options = []node.Option{
		node.WithDisableAPI(false),
		node.WithDisableP2P(false),
		node.WithStorePath(t.TempDir()),
		http.WithAddress(defraUrl),
		netConfig.WithListenAddresses(listenAddress),
	}
	readerDefra := StartDefraInstance(t, ctx, options)
	defer readerDefra.Close(ctx)

	addSchema(t, ctx, writerDefra)
	addSchema(t, ctx, readerDefra)

	postBasicData(t, ctx, writerDefra)

	result, err := getUserName(ctx, writerDefra)
	require.NoError(t, err)
	require.Equal(t, "Quinn", result)

	time.Sleep(10 * time.Second) // Allow some time to give the data a chance to sync to the readerDefra instance (it won't since they aren't connected, but we give it time anyways just in case)

	result, err = getUserName(ctx, readerDefra)
	require.Error(t, err)

	err = writerDefra.DB.SetReplicator(ctx, readerDefra.DB.PeerInfo())
	require.NoError(t, err)

	result, err = getUserName(ctx, readerDefra)
	for attempts := 1; attempts < 100; attempts++ { // It may take some time to sync now that we are connected
		if err == nil {
			break
		}
		t.Logf("Attempt %d to query username from readerDefra failed. Trying again...", attempts)
		time.Sleep(1 * time.Second)
		result, err = getUserName(ctx, readerDefra)
	}
	require.NoError(t, err)
	require.Equal(t, "Quinn", result)
}

func getUserName(ctx context.Context, readerDefra *node.Node) (string, error) {
	query := `query GetUserName{
		User(limit: 1) {
			name
		}
	}`
	user, err := defra.QuerySingle[UserWithVersion](ctx, readerDefra, query)
	if err != nil {
		return "", fmt.Errorf("Error querying user: %v", err)
	}
	if len(user.Name) == 0 && len(user.Version) == 0 {
		return "", fmt.Errorf("No users found")
	}
	return user.Name, nil
}

func addSchema(t *testing.T, ctx context.Context, writerDefra *node.Node) {
	schema := "type User { name: String }"
	_, err := writerDefra.DB.AddSchema(ctx, schema)
	require.NoError(t, err)
}

func postBasicData(t *testing.T, ctx context.Context, writerDefra *node.Node) {
	query := `mutation {
		create_User(input: { name: "Quinn" }) {
			name
		}
	}`

	result, err := defra.PostMutation[UserWithVersion](ctx, writerDefra, query)
	require.NoError(t, err)
	require.Equal(t, "Quinn", result.Name)
}

// This test shows us what active replication looks like with multiple tenants
// The test spins up many defra instances. The first instance will write some basic data.
// From here, we essentially form a chain of defra instances, all of which want to read the data.
// Data gets propogated down the chain of defra instances by having each defra instance in turn set the next defra instance as a replicator.
// i.e. Writer Node -> replicates to Node B -> replicates to Node C -> replicates to Node D -> ...
func TestMultiTenantP2PReplication_ManualReplicatorAssignment(t *testing.T) {
	listenAddress := "/ip4/127.0.0.1/tcp/0"
	defraUrl := "127.0.0.1:0"
	ctx := context.Background()
	writerDefra := createWriterDefraInstanceAndPostBasicData(t, ctx, defraUrl, listenAddress)
	defer writerDefra.Close(ctx)

	previousDefra := writerDefra
	readerDefraInstances := []*node.Node{}
	for i := 0; i < 10; i++ {
		readerDefraOptions := []node.Option{
			node.WithDisableAPI(false),
			node.WithDisableP2P(false),
			node.WithStorePath(t.TempDir()),
			http.WithAddress(defraUrl),
			netConfig.WithListenAddresses(listenAddress),
		}
		newDefraInstance := StartDefraInstance(t, ctx, readerDefraOptions)
		defer newDefraInstance.Close(ctx)

		assertDefraInstanceDoesNotHaveData(t, ctx, newDefraInstance)

		addSchema(t, ctx, newDefraInstance)

		assertDefraInstanceDoesNotHaveData(t, ctx, newDefraInstance)

		err := previousDefra.DB.SetReplicator(ctx, newDefraInstance.DB.PeerInfo())
		require.NoError(t, err)
		readerDefraInstances = append(readerDefraInstances, newDefraInstance)
		previousDefra = newDefraInstance
	}

	assertReaderDefraInstancesHaveLatestData(t, ctx, readerDefraInstances)
}

// This test shows us what passive replication looks like with multiple tenants
// The test mirrors TestMultiTenantP2PReplication_ManualReplicatorAssignment in terms of setup
// However, in this test, instead of using Active Replication, we form a connection and use Passive Replication
// i.e. Writer Node -> replicates to Node B -> replicates to Node C -> replicates to Node D -> ...
func TestMultiTenantP2PReplication_ConnectToPeers(t *testing.T) {
	listenAddress := "/ip4/127.0.0.1/tcp/0"
	defraUrl := "127.0.0.1:0"
	ctx := context.Background()
	writerDefra := createWriterDefraInstanceAndApplySchema(t, ctx, defraUrl, listenAddress)
	defer writerDefra.Close(ctx)
	err := writerDefra.DB.AddP2PCollections(ctx, "User")
	require.NoError(t, err)

	previousDefra := writerDefra
	readerDefraInstances := []*node.Node{}
	for i := 0; i < 10; i++ {
		readerDefraOptions := []node.Option{
			node.WithDisableAPI(false),
			node.WithDisableP2P(false),
			node.WithStorePath(t.TempDir()),
			http.WithAddress(defraUrl),
			netConfig.WithListenAddresses(listenAddress),
		}
		newDefraInstance := StartDefraInstance(t, ctx, readerDefraOptions)
		defer newDefraInstance.Close(ctx)

		err = newDefraInstance.DB.Connect(ctx, previousDefra.DB.PeerInfo())
		require.NoError(t, err)

		addSchema(t, ctx, newDefraInstance)

		err = newDefraInstance.DB.AddP2PCollections(ctx, "User")
		require.NoError(t, err)

		readerDefraInstances = append(readerDefraInstances, newDefraInstance)
		previousDefra = newDefraInstance
	}

	postBasicData(t, ctx, writerDefra)

	result, err := getUserName(ctx, writerDefra)
	require.NoError(t, err)
	require.Equal(t, "Quinn", result)

	assertReaderDefraInstancesHaveLatestData(t, ctx, readerDefraInstances)
}

func createWriterDefraInstanceAndPostBasicData(t *testing.T, ctx context.Context, defraUrl string, listenAddress string) *node.Node {
	writerDefra := createWriterDefraInstanceAndApplySchema(t, ctx, defraUrl, listenAddress)

	postBasicData(t, ctx, writerDefra)

	result, err := getUserName(ctx, writerDefra)
	require.NoError(t, err)
	require.Equal(t, "Quinn", result)

	return writerDefra
}

func createWriterDefraInstanceAndApplySchema(t *testing.T, ctx context.Context, defraUrl string, listenAddress string) *node.Node {
	options := []node.Option{
		node.WithDisableAPI(false),
		node.WithDisableP2P(false),
		node.WithStorePath(t.TempDir()),
		http.WithAddress(defraUrl),
		netConfig.WithListenAddresses(listenAddress),
	}
	writerDefra := StartDefraInstance(t, ctx, options)

	addSchema(t, ctx, writerDefra)
	return writerDefra
}

func createDefraInstanceAndApplySchema(t *testing.T, ctx context.Context, options []node.Option) *node.Node {
	instance := StartDefraInstance(t, ctx, options)
	addSchema(t, ctx, instance)
	return instance
}

func assertReaderDefraInstancesHaveLatestData(t *testing.T, ctx context.Context, readerDefraInstances []*node.Node) {
	for i, readerDefra := range readerDefraInstances {
		result, err := getUserName(ctx, readerDefra)
		for attempts := 1; attempts < 60; attempts++ { // It may take some time to sync now that we are connected
			if err == nil {
				break
			}
			t.Logf("Attempt %d to query username from readerDefra %d failed. Trying again...", attempts, i)
			time.Sleep(1 * time.Second)
			result, err = getUserName(ctx, readerDefra)
		}
		require.NoError(t, err, fmt.Sprintf("Received unexpected error when checking user name for node %d: %v", i, err))
		require.Equal(t, "Quinn", result)
	}
}

// This test aims to mimic more closely the setup we will have for Shinzo
// This test introduces the concept of a "big peer"
// The "big peer" serves as an entrypoint into the P2P network
// In this test, we have a defra node that writes data and many defra nodes that want to read the data
// Each node forms a connection with the "big peer" when it starts, then data is passively replicated to each of them
// The data travels from writerNode -> big peer -> each of the readerNodes
func TestMultiTenantP2PReplication_ConnectToBigPeer(t *testing.T) {
	listenAddress := "/ip4/127.0.0.1/tcp/0"
	defraUrl := "127.0.0.1:0"
	ctx := context.Background()

	bigPeer := createWriterDefraInstanceAndApplySchema(t, ctx, defraUrl, listenAddress)
	defer bigPeer.Close(ctx)
	err := bigPeer.DB.AddP2PCollections(ctx, "User")
	require.NoError(t, err)

	options := []node.Option{
		node.WithDisableAPI(false),
		node.WithDisableP2P(false),
		node.WithStorePath(t.TempDir()),
		http.WithAddress(defraUrl),
		netConfig.WithListenAddresses(listenAddress),
	}
	writerDefra := createDefraInstanceAndApplySchema(t, ctx, options)
	defer writerDefra.Close(ctx)
	err = writerDefra.DB.AddP2PCollections(ctx, "User")
	require.NoError(t, err)

	err = writerDefra.DB.Connect(ctx, bigPeer.DB.PeerInfo())
	require.NoError(t, err)

	readerDefraInstances := []*node.Node{}
	for i := 0; i < 10; i++ {
		readerDefraOptions := []node.Option{
			node.WithDisableAPI(false),
			node.WithDisableP2P(false),
			node.WithStorePath(t.TempDir()),
			http.WithAddress(defraUrl),
			netConfig.WithListenAddresses(listenAddress),
		}
		newDefraInstance := StartDefraInstance(t, ctx, readerDefraOptions)
		defer newDefraInstance.Close(ctx)

		assertDefraInstanceDoesNotHaveData(t, ctx, newDefraInstance)

		err = newDefraInstance.DB.Connect(ctx, bigPeer.DB.PeerInfo())
		require.NoError(t, err)

		addSchema(t, ctx, newDefraInstance)

		assertDefraInstanceDoesNotHaveData(t, ctx, newDefraInstance)

		err = newDefraInstance.DB.AddP2PCollections(ctx, "User")
		require.NoError(t, err)

		readerDefraInstances = append(readerDefraInstances, newDefraInstance)
	}

	postBasicData(t, ctx, writerDefra)

	result, err := getUserName(ctx, writerDefra)
	require.NoError(t, err)
	require.Equal(t, "Quinn", result)

	assertReaderDefraInstancesHaveLatestData(t, ctx, readerDefraInstances)
}

func assertDefraInstanceDoesNotHaveData(t *testing.T, ctx context.Context, readerDefra *node.Node) {
	_, err := getUserName(ctx, readerDefra)
	require.Error(t, err)
}

// This test mirrors the setup of TestMultiTenantP2PReplication_ConnectToBigPeer
// Except, in this test, our "big peer" doesn't subscribe to any collections
// With the "big peer" not subscribed to the "User" collection, data is not passively replicated to it
// We also see that the data does not "hop" past the "big peer" and form a connection from the writer to the readers
// The readers do not receive the data they are looking for
func TestMultiTenantP2PReplication_ConnectToBigPeerWhoDoesNotDeclareInterestInTopics(t *testing.T) {
	listenAddress := "/ip4/127.0.0.1/tcp/0"
	defraUrl := "127.0.0.1:0"
	ctx := context.Background()

	bigPeer := createWriterDefraInstanceAndApplySchema(t, ctx, defraUrl, listenAddress)
	defer bigPeer.Close(ctx)
	// Notice the big peer does not add any P2P Collections

	options := []node.Option{
		node.WithDisableAPI(false),
		node.WithDisableP2P(false),
		node.WithStorePath(t.TempDir()),
		http.WithAddress(defraUrl),
		netConfig.WithListenAddresses(listenAddress),
	}
	writerDefra := createDefraInstanceAndApplySchema(t, ctx, options)
	defer writerDefra.Close(ctx)
	err := writerDefra.DB.AddP2PCollections(ctx, "User")
	require.NoError(t, err)

	err = writerDefra.DB.Connect(ctx, bigPeer.DB.PeerInfo())
	require.NoError(t, err)

	readerDefraInstances := []*node.Node{}
	for i := 0; i < 10; i++ {
		readerDefraOptions := []node.Option{
			node.WithDisableAPI(false),
			node.WithDisableP2P(false),
			node.WithStorePath(t.TempDir()),
			http.WithAddress(defraUrl),
			netConfig.WithListenAddresses(listenAddress),
		}
		newDefraInstance := StartDefraInstance(t, ctx, readerDefraOptions)
		defer newDefraInstance.Close(ctx)

		assertDefraInstanceDoesNotHaveData(t, ctx, newDefraInstance)

		err = newDefraInstance.DB.Connect(ctx, bigPeer.DB.PeerInfo())
		require.NoError(t, err)

		addSchema(t, ctx, newDefraInstance)

		assertDefraInstanceDoesNotHaveData(t, ctx, newDefraInstance)

		err = newDefraInstance.DB.AddP2PCollections(ctx, "User")
		require.NoError(t, err)

		readerDefraInstances = append(readerDefraInstances, newDefraInstance)
	}

	postBasicData(t, ctx, writerDefra)

	result, err := getUserName(ctx, writerDefra)
	require.NoError(t, err)
	require.Equal(t, "Quinn", result)

	for i := 0; i < 60; i++ {
		time.Sleep(1 * time.Second)
		for _, reader := range readerDefraInstances {
			assertDefraInstanceDoesNotHaveData(t, ctx, reader) // In general, it may take some time for data to passively replicate so we give it a chance to - however, it won't in this test due to the setup
		}
	}
}

// This test has multiple defra nodes writing the same data to be read by another node
// This closely mimics the Shinzo setup, where multiple Indexers and Hosts will be writing the same data
// This test allows us to explore what the built-in `__version` field defra provides will look like in this case,
// Informing our design for the attestation system
// The key observation is that we receive a `[]Version` whose length is equal to the number of writers (and unique signatures)
func TestSyncFromMultipleWriters(t *testing.T) {
	listenAddress := "/ip4/127.0.0.1/tcp/0"
	defraUrl := "127.0.0.1:0"
	ctx := context.Background()

	writerDefras := []*node.Node{}
	defraNodes := 10
	for i := 0; i < defraNodes; i++ {
		nodeIdentity, err := identity.Generate(crypto.KeyTypeSecp256k1)
		require.NoError(t, err)
		options := []node.Option{
			node.WithDisableAPI(false),
			node.WithDisableP2P(false),
			node.WithStorePath(t.TempDir()),
			http.WithAddress(defraUrl),
			netConfig.WithListenAddresses(listenAddress),
			node.WithNodeIdentity(identity.Identity(nodeIdentity)),
		}
		writerDefra := StartDefraInstance(t, ctx, options)
		defer writerDefra.Close(ctx)

		addSchema(t, ctx, writerDefra)
		err = writerDefra.DB.AddP2PCollections(ctx, "User")
		require.NoError(t, err)

		writerDefras = append(writerDefras, writerDefra)
	}

	// Create a reader instance
	readerOptions := []node.Option{
		node.WithDisableAPI(false),
		node.WithDisableP2P(false),
		node.WithStorePath(t.TempDir()),
		http.WithAddress(defraUrl),
		netConfig.WithListenAddresses(listenAddress),
	}
	readerDefra := StartDefraInstance(t, ctx, readerOptions)
	defer readerDefra.Close(ctx)

	for _, writer := range writerDefras {
		err := readerDefra.DB.Connect(ctx, writer.DB.PeerInfo())
		require.NoError(t, err)
	}

	addSchema(t, ctx, readerDefra)

	assertDefraInstanceDoesNotHaveData(t, ctx, readerDefra)

	err := readerDefra.DB.AddP2PCollections(ctx, "User")
	require.NoError(t, err)

	// Write data to each writer
	for _, writer := range writerDefras {
		postBasicData(t, ctx, writer)

		// Verify the data was written to this writer
		result, err := getUserName(ctx, writer)
		require.NoError(t, err)
		require.Equal(t, "Quinn", result)
	}

	// Wait for sync and verify reader has all data
	result, err := getUserName(ctx, readerDefra)
	for attempts := 1; attempts < 60; attempts++ { // It may take some time to sync now that we are connected
		if err == nil {
			break
		}
		t.Logf("Attempt %d to query username from readerDefra failed. Trying again...", attempts)
		time.Sleep(1 * time.Second)
		result, err = getUserName(ctx, readerDefra)
	}
	require.Equal(t, "Quinn", result)

	userWithVersion, err := getUserWithVersion(ctx, readerDefra)
	require.NoError(t, err)
	require.Equal(t, "Quinn", userWithVersion.Name)
	require.Equal(t, defraNodes, len(userWithVersion.Version))
}

type UserWithVersion struct {
	Name    string                `json:"name"`
	Version []attestation.Version `json:"_version"`
}

func getUserWithVersion(ctx context.Context, defraNode *node.Node) (UserWithVersion, error) {
	query := `query GetUserWithVersion{
		User(limit: 1) {
			name
			_version {
				cid
				signature {
					type
					identity
					value
					__typename
				}
			}
		}
	}`

	user, err := defra.QuerySingle[UserWithVersion](ctx, defraNode, query)
	if err != nil {
		return UserWithVersion{}, fmt.Errorf("Error querying user with version: %v", err)
	}

	return user, nil
}
