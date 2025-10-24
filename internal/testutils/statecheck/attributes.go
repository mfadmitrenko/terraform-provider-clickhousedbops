package statecheck

import (
	"context"
	"fmt"

	tfjson "github.com/hashicorp/terraform-json"
	"github.com/hashicorp/terraform-plugin-testing/statecheck"

	"github.com/ClickHouse/terraform-provider-clickhousedbops/internal/dbops"
)

type getAttributes[T dbops.Role] struct {
	resourceAddress string
	callback        func(map[string]interface{}) error
}

func NewGetAttributes[T dbops.Role](resourceAddress string, callback func(map[string]interface{}) error) statecheck.StateCheck {
	return &getAttributes[T]{
		resourceAddress: resourceAddress,
		callback:        callback,
	}
}

func (e getAttributes[T]) CheckState(ctx context.Context, req statecheck.CheckStateRequest, resp *statecheck.CheckStateResponse) {
	var tfresource *tfjson.StateResource

	if req.State == nil {
		resp.Error = fmt.Errorf("state is nil")
		return
	}

	if req.State.Values == nil {
		resp.Error = fmt.Errorf("state does not contain any state values")

		return
	}

	if req.State.Values.RootModule == nil {
		resp.Error = fmt.Errorf("state does not contain a root module")

		return
	}

	for _, r := range req.State.Values.RootModule.Resources {
		if e.resourceAddress == r.Address {
			tfresource = r

			break
		}
	}

	if tfresource == nil {
		resp.Error = fmt.Errorf("%s - Resource not found in state", e.resourceAddress)

		return
	}

	err := e.callback(tfresource.AttributeValues)
	if err != nil {
		resp.Error = err
		return
	}
}
