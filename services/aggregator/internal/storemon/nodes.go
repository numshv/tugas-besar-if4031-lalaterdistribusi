package storemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

const (
	memberSelector = "bnpb/mongo-member=true"
	unknownNode    = "unknown"
)

type NodeResolver interface {
	Node(u Unit) string
}

type SlotNodes struct{}

func (SlotNodes) Node(u Unit) string {
	return "slot-" + u.Slot
}

type KubeNodes struct {
	pods corelisters.PodLister
}

func NewKubeNodes(ctx context.Context, client kubernetes.Interface, namespace string) (*KubeNodes, error) {
	factory := informers.NewSharedInformerFactoryWithOptions(client, 0,
		informers.WithNamespace(namespace),
		informers.WithTweakListOptions(func(o *metav1.ListOptions) { o.LabelSelector = memberSelector }))
	pods := factory.Core().V1().Pods()
	informer := pods.Informer()
	factory.Start(ctx.Done())

	syncCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if !cache.WaitForCacheSync(syncCtx.Done(), informer.HasSynced) {
		return nil, errors.New("pod informer did not sync")
	}
	return &KubeNodes{pods: pods.Lister()}, nil
}

func (k *KubeNodes) Node(u Unit) string {
	selector := labels.SelectorFromSet(labels.Set{"bnpb/replset": u.ReplicaSet, "bnpb/slot": u.Slot})
	list, err := k.pods.List(selector)
	if err != nil {
		return unknownNode
	}
	for _, p := range list {
		if p.Spec.NodeName != "" {
			return p.Spec.NodeName
		}
	}
	return unknownNode
}

func NodesFromEnvironment(ctx context.Context, namespace string, log *slog.Logger) NodeResolver {
	if os.Getenv("KUBERNETES_SERVICE_HOST") == "" {
		log.InfoContext(ctx, "not running in kubernetes, storage units report their slot as node")
		return SlotNodes{}
	}
	resolver, err := inCluster(ctx, namespace)
	if err != nil {
		log.WarnContext(ctx, "cannot watch mongo pods, storage units report their slot as node", "error", err)
		return SlotNodes{}
	}
	return resolver
}

func inCluster(ctx context.Context, namespace string) (*KubeNodes, error) {
	if namespace == "" {
		return nil, errors.New("POD_NAMESPACE is empty")
	}
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("in-cluster config: %w", err)
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("kubernetes client: %w", err)
	}
	return NewKubeNodes(ctx, client, namespace)
}
