package v1

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/robomotionio/robomotion-go/message"
	"github.com/robomotionio/robomotion-go/runtime"
)

// SeaTableGetMetadata retrieves the metadata (tables, columns structure) of a base.
type SeaTableGetMetadata struct {
	runtime.Node `spec:"id=Robomotion.SeaTable.GetMetadata,name=Get Metadata,icon=mdiDatabaseCog,color=#00C2E0,inputs=1,outputs=1"`

	InClientID runtime.InVariable[string] `spec:"title=Client ID,type=string,scope=Message,name=clientId,messageScope,jsScope,customScope"`

	OutStatusCode runtime.OutVariable[int]    `spec:"title=Status Code,type=int,scope=Message,name=statusCode,messageScope"`
	OutRaw        runtime.OutVariable[string] `spec:"title=Raw Body,type=string,scope=Message,name=body,messageScope"`
	OutJSON       runtime.OutVariable[any]    `spec:"title=JSON,type=object,scope=Message,name=json,messageScope"`
	OutTables     runtime.OutVariable[any]    `spec:"title=Tables,type=object,scope=Message,name=tables,messageScope"`
}

func (n *SeaTableGetMetadata) OnCreate() error { return nil }
func (n *SeaTableGetMetadata) OnClose() error  { return nil }

func (n *SeaTableGetMetadata) OnMessage(ctx message.Context) error {
	clientID, err := n.InClientID.Get(ctx)
	if err != nil {
		return err
	}
	cfg, ok := getSeaTableClient(clientID)
	if !ok {
		return runtime.NewError("ErrInvalidArg", "Unknown Client ID – run SeaTable.Connect first")
	}

	url := fmt.Sprintf("%s/api-gateway/api/v2/dtables/%s/metadata/", cfg.Server, cfg.BaseUUID)
	respBody, status, err := doSeaTableRequest(context.Background(), "GET", url, cfg.Token, nil)
	if err != nil {
		return err
	}

	n.OutStatusCode.Set(ctx, status)
	n.OutRaw.Set(ctx, string(respBody))

	var parsed any
	if err := json.Unmarshal(respBody, &parsed); err == nil {
		n.OutJSON.Set(ctx, parsed)
	}

	// Extract tables array from metadata
	var tables any
	if m, ok := parsed.(map[string]any); ok {
		if metadata, ok := m["metadata"].(map[string]any); ok {
			if t, ok := metadata["tables"]; ok {
				tables = t
			}
		}
		// Fallback: tables might be at root level
		if tables == nil {
			if t, ok := m["tables"]; ok {
				tables = t
			}
		}
	}
	if tables == nil {
		tables = []any{}
	}
	n.OutTables.Set(ctx, tables)

	return nil
}

