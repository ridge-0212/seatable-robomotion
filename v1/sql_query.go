package v1

import (
    "context"
    "encoding/json"
    "fmt"
    "strings"

    "github.com/robomotionio/robomotion-go/message"
    "github.com/robomotionio/robomotion-go/runtime"
)

// SeaTableSQLQuery executes arbitrary SQL against a SeaTable base.
type SeaTableSQLQuery struct {
    runtime.Node `spec:"id=Robomotion.SeaTable.SQLQuery,name=SQL Query,icon=mdiCodeBraces,color=#00C2E0,inputs=1,outputs=1"`

    InClientID runtime.InVariable[string]  `spec:"title=Client ID,type=string,scope=Message,name=clientId,messageScope,jsScope,customScope"`
    InSQL      runtime.InVariable[string]  `spec:"title=SQL,type=string,scope=Message,name=sql,messageScope,jsScope,customScope"`
    OptParams  runtime.OptVariable[any]    `spec:"title=Params,type=object,scope=Message,name=params,messageScope,customScope,jsScope"`
    OptConvert runtime.OptVariable[bool]   `spec:"title=Convert Keys,type=bool,value=true,scope=Message,name=convertKeys,messageScope,customScope,jsScope"`

    OutStatusCode runtime.OutVariable[int]         `spec:"title=Status Code,type=int,scope=Message,name=statusCode,messageScope"`
    OutRaw        runtime.OutVariable[string]      `spec:"title=Raw Body,type=string,scope=Message,name=body,messageScope"`
    OutJSON       runtime.OutVariable[any]         `spec:"title=JSON,type=object,scope=Message,name=json,messageScope"`
}

func (n *SeaTableSQLQuery) OnCreate() error { return nil }
func (n *SeaTableSQLQuery) OnClose() error  { return nil }

func (n *SeaTableSQLQuery) OnMessage(ctx message.Context) error {
    clientID, err := n.InClientID.Get(ctx)
    if err != nil {
        return err
    }
    cfg, ok := getSeaTableClient(clientID)
    if !ok {
        return runtime.NewError("ErrInvalidArg", "Unknown Client ID – run SeaTable.Connect first")
    }

    sqlText, err := n.InSQL.Get(ctx)
    if err != nil {
        return err
    }
    if strings.TrimSpace(sqlText) == "" {
        return runtime.NewError("ErrInvalidArg", "SQL is required")
    }

    var params []any
    if v, err := n.OptParams.Get(ctx); err == nil && v != nil {
        switch t := v.(type) {
        case []any:
            params = t
        default:
            b, _ := json.Marshal(v)
            _ = json.Unmarshal(b, &params)
        }
    }

    convert, _ := n.OptConvert.Get(ctx)

    body := map[string]any{
        "sql": sqlText,
    }
    if len(params) > 0 {
        body["params"] = params
    }
    body["convert_keys"] = convert

    url := fmt.Sprintf("%s/api-gateway/api/v2/dtables/%s/sql/", cfg.Server, cfg.BaseUUID)
    respBody, status, err := doSeaTableRequest(context.Background(), "POST", url, cfg.Token, body)
    if err != nil {
        return err
    }

    n.OutStatusCode.Set(ctx, status)
    n.OutRaw.Set(ctx, string(respBody))

    var parsed any
    if err := json.Unmarshal(respBody, &parsed); err == nil {
        n.OutJSON.Set(ctx, parsed)
    }
    return nil
}
