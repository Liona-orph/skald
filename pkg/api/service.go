package api

import "context"

// Service is the complete set of operations Skald exposes.
//
// One interface is implemented by three things: the in-process engine, the HTTP
// handler that adapts it to the network, and the HTTP client the SDK uses. That
// symmetry is worth a lot -- a worker can be pointed at an embedded engine with
// no server at all, which is what makes the test suite fast and makes
// single-binary deployments possible without a second code path.
type Service interface {
	StartWorkflow(ctx context.Context, req StartWorkflowRequest) (StartWorkflowResponse, error)
	SignalWorkflow(ctx context.Context, req SignalWorkflowRequest) error
	SignalWithStartWorkflow(ctx context.Context, req SignalWithStartRequest) (StartWorkflowResponse, error)
	CancelWorkflow(ctx context.Context, req CancelWorkflowRequest) error
	TerminateWorkflow(ctx context.Context, req TerminateWorkflowRequest) error
	DescribeWorkflow(ctx context.Context, namespace, workflowID, runID string) (DescribeWorkflowResponse, error)
	GetHistory(ctx context.Context, req GetHistoryRequest) (GetHistoryResponse, error)
	ListWorkflows(ctx context.Context, req ListWorkflowsRequest) (ListWorkflowsResponse, error)

	PollWorkflowTask(ctx context.Context, req PollWorkflowTaskRequest) (WorkflowTask, error)
	RespondWorkflowTaskCompleted(ctx context.Context, req RespondWorkflowTaskCompletedRequest) error
	RespondWorkflowTaskFailed(ctx context.Context, req RespondWorkflowTaskFailedRequest) error

	PollActivityTask(ctx context.Context, req PollActivityTaskRequest) (ActivityTask, error)
	RespondActivityTaskCompleted(ctx context.Context, req RespondActivityTaskCompletedRequest) error
	RespondActivityTaskFailed(ctx context.Context, req RespondActivityTaskFailedRequest) error
	RespondActivityTaskCanceled(ctx context.Context, req RespondActivityTaskCanceledRequest) error
	RecordActivityHeartbeat(ctx context.Context, req RecordActivityHeartbeatRequest) (RecordActivityHeartbeatResponse, error)
}
