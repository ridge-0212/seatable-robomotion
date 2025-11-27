package v1

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/robomotionio/robomotion-go/message"
	"github.com/robomotionio/robomotion-go/runtime"
)

// SeaTableListViews lists all views of a specific table.
type SeaTableListViews struct {
	runtime.Node `spec:"id=Robomotion.SeaTable.ListViews,name=List Views,icon=mdiViewList,color=#00C2E0,inputs=1,outputs=1"`

	InClientID  runtime.InVariable[string] `spec:"title=Client ID,type=string,scope=Message,name=clientId,messageScope,jsScope,customScope"`
	InTableName runtime.InVariable[string] `spec:"title=Table Name,type=string,scope=Message,name=tableName,messageScope,jsScope,customScope"`

	OutStatusCode runtime.OutVariable[int]    `spec:"title=Status Code,type=int,scope=Message,name=statusCode,messageScope"`
	OutRaw        runtime.OutVariable[string] `spec:"title=Raw Body,type=string,scope=Message,name=body,messageScope"`
	OutJSON       runtime.OutVariable[any]    `spec:"title=JSON,type=object,scope=Message,name=json,messageScope"`
	OutViews      runtime.OutVariable[any]    `spec:"title=Views,type=object,scope=Message,name=views,messageScope"`
	OutCount      runtime.OutVariable[int]    `spec:"title=Count,type=int,scope=Message,name=count,messageScope"`
}

func (n *SeaTableListViews) OnCreate() error { return nil }
func (n *SeaTableListViews) OnClose() error  { return nil }

func (n *SeaTableListViews) OnMessage(ctx message.Context) error {
	clientID, err := n.InClientID.Get(ctx)
	if err != nil {
		return err
	}
	cfg, ok := getSeaTableClient(clientID)
	if !ok {
		return runtime.NewError("ErrInvalidArg", "Unknown Client ID – run SeaTable.Connect first")
	}

	tableName, err := n.InTableName.Get(ctx)
	if err != nil {
		return err
	}
	tableName = strings.TrimSpace(tableName)
	if tableName == "" {
		return runtime.NewError("ErrInvalidArg", "Table Name is required")
	}

	u, err := url.Parse(fmt.Sprintf("%s/api-gateway/api/v2/dtables/%s/views/", cfg.Server, cfg.BaseUUID))
	if err != nil {
		return fmt.Errorf("parse URL: %w", err)
	}
	q := u.Query()
	q.Set("table_name", tableName)
	u.RawQuery = q.Encode()

	respBody, status, err := doSeaTableRequest(context.Background(), "GET", u.String(), cfg.Token, nil)
	if err != nil {
		return err
	}

	n.OutStatusCode.Set(ctx, status)
	n.OutRaw.Set(ctx, string(respBody))

	var parsed any
	if err := json.Unmarshal(respBody, &parsed); err == nil {
		n.OutJSON.Set(ctx, parsed)
	}

	// Extract views array
	var views []any
	if m, ok := parsed.(map[string]any); ok {
		if v, ok := m["views"].([]any); ok {
			views = v
		}
	}
	if views == nil {
		views = []any{}
	}
	n.OutViews.Set(ctx, views)
	n.OutCount.Set(ctx, len(views))

	return nil
}

