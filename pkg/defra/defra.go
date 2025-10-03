package defra

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/shinzonetwork/app-sdk/pkg/config"
	"github.com/shinzonetwork/app-sdk/pkg/logger"
	"github.com/sourcenetwork/defradb/acp/identity"
	"github.com/sourcenetwork/defradb/crypto"
	"github.com/sourcenetwork/defradb/http"
	netConfig "github.com/sourcenetwork/defradb/net/config"
	"github.com/sourcenetwork/defradb/node"
)

var DefaultConfig *config.Config = &config.Config{
	DefraDB: config.DefraDBConfig{
		Url:           "http://localhost:9181",
		KeyringSecret: os.Getenv("DEFRA_KEYRING_SECRET"),
		P2P: config.DefraP2PConfig{
			BootstrapPeers: requiredPeers,
			ListenAddr:     defaultListenAddress,
		},
		Store: config.DefraStoreConfig{
			Path: ".defra",
		},
	},
	Logger: config.LoggerConfig{
		Development: false,
	},
}

var requiredPeers []string = []string{} // Here, we can add some "big peers" to give nodes a starting place when building their peer network
const defaultListenAddress string = "/ip4/127.0.0.1/tcp/9171"

func StartDefraInstance(cfg *config.Config, schemaApplier SchemaApplier, collectionsOfInterest ...string) (*node.Node, error) {
	ctx := context.Background()

	if cfg == nil {
		return nil, fmt.Errorf("Config cannot be nil")
	}
	cfg.DefraDB.P2P.BootstrapPeers = append(cfg.DefraDB.P2P.BootstrapPeers, requiredPeers...)

	logger.Init(cfg.Logger.Development)

	nodeIdentity, err := identity.Generate(crypto.KeyTypeSecp256k1) // Todo: this is an ephemeral identity - this means that each time we start a defra instance via this method, it will have a randomly generated signing key - we'll want to add keyring support
	if err != nil {
		return nil, fmt.Errorf("Error generating identity: %v", err)
	}

	// Create defra node
	options := []node.Option{
		node.WithDisableAPI(false),
		node.WithDisableP2P(false),
		node.WithStorePath(cfg.DefraDB.Store.Path),
		http.WithAddress(strings.Replace(cfg.DefraDB.Url, "http://localhost", "127.0.0.1", 1)),
		node.WithNodeIdentity(identity.Identity(nodeIdentity)),
	}
	listenAddress := cfg.DefraDB.P2P.ListenAddr
	if len(listenAddress) > 0 {
		options = append(options, netConfig.WithListenAddresses(listenAddress))
	}
	defraNode, err := node.New(ctx, options...)
	if err != nil {
		return nil, fmt.Errorf("Failed to create defra node: %v ", err)
	}

	err = defraNode.Start(ctx)
	if err != nil {
		return nil, fmt.Errorf("Failed to start defra node: %v ", err)
	}

	// Connect to bootstrap peers
	peers, errors := bootstrapIntoPeers(cfg.DefraDB.P2P.BootstrapPeers)
	for _, err := range errors {
		logger.Sugar.Errorf("Error translating bootstrapped peers: %v", err)
	}
	errors = connectToPeers(ctx, defraNode, peers)
	if len(errors) > 0 {
		if len(errors) == len(peers) {
			defer defraNode.Close(ctx)
			return nil, fmt.Errorf("Failed to connect to any peers, with errors: %v", errors)
		}
		logger.Sugar.Errorf("Failed to connect to %d peers, with errors: %v", len(errors), errors)
	}

	err = schemaApplier.ApplySchema(ctx, defraNode)
	if err != nil {
		if strings.Contains(err.Error(), "collection already exists") {
			logger.Sugar.Warnf("Failed to apply schema: %v\nProceeding...", err)
		} else {
			defer defraNode.Close(ctx)
			return nil, fmt.Errorf("Failed to apply schema: %v", err)
		}
	}

	err = defraNode.DB.AddP2PCollections(ctx, collectionsOfInterest...)
	if err != nil {
		return nil, fmt.Errorf("Failed to add collections of interes %v: %w", collectionsOfInterest, err)
	}

	return defraNode, nil
}

// A simple wrapper on StartDefraInstance that changes the configured defra store path to a temp directory for the test
func StartDefraInstanceWithTestConfig(t *testing.T, cfg *config.Config, schemaApplier SchemaApplier) (*node.Node, error) {
	cfg.DefraDB.Store.Path = t.TempDir()
	cfg.DefraDB.Url = "127.0.0.1:0"
	return StartDefraInstance(cfg, schemaApplier)
}
