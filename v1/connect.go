package v1

import (
	"strings"

	"github.com/robomotionio/robomotion-go/message"
	"github.com/robomotionio/robomotion-go/runtime"
)

// SeaTableConnect creates a SeaTable client and outputs a Client ID for reuse.
type SeaTableConnect struct {
	runtime.Node `spec:"id=Robomotion.SeaTable.Connect,name=Connect,icon=mdiDatabase,color=#00C2E0,inputs=1,outputs=1"`

	InServer   runtime.InVariable[string] `spec:"title=Server URL,type=string,scope=Message,name=serverUrl,messageScope,customScope,jsScope"`
	InBaseUUID runtime.InVariable[string] `spec:"title=Base UUID,type=string,scope=Message,name=baseUuid,messageScope,customScope,jsScope"`

	OptBaseToken runtime.Credential `spec:"title=Base Token,scope=Custom,category=4,messageScope,customScope"`

	OutClientID runtime.OutVariable[string] `spec:"title=Client ID,type=string,scope=Message,name=clientId,messageScope"`
}

func (n *SeaTableConnect) OnCreate() error { return nil }
func (n *SeaTableConnect) OnClose() error  { return nil }

func (n *SeaTableConnect) OnMessage(ctx message.Context) error {
	server, err := n.InServer.Get(ctx)
	if err != nil {
		return err
	}
	baseUUID, err := n.InBaseUUID.Get(ctx)
	if err != nil {
		return err
	}

	server = trimTrailingSlash(server)
	baseUUID = strings.TrimSpace(baseUUID)
	if server == "" {
		return runtime.NewError("ErrInvalidArg", "Server URL is required")
	}
	if baseUUID == "" {
		return runtime.NewError("ErrInvalidArg", "Base UUID is required")
	}

	item, err := n.OptBaseToken.Get(ctx)
	if err != nil {
		return err
	}
	if item == nil {
		return runtime.NewError("ErrInvalidArg", "Base Token is required")
	}
	v, ok := item["value"]
	if !ok {
		return runtime.NewError("ErrInvalidArg", "Vault item missing 'value'")
	}
	token, ok := v.(string)
	if !ok || strings.TrimSpace(token) == "" {
		return runtime.NewError("ErrInvalidArg", "Invalid Base Token value")
	}

	cfg := &SeaTableClient{
		Server:   server,
		BaseUUID: baseUUID,
		Token:    token,
	}
	clientID := registerSeaTableClient(cfg)

	if err := n.OutClientID.Set(ctx, clientID); err != nil {
		return err
	}
	return nil
}
