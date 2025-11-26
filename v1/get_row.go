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

// SeaTableGetRow fetches a single row by ID, optionally constrained by view.
type SeaTableGetRow struct {
    runtime.Node `spec:"id=SeaTable.GetRow,name=Get Row,icon=mdiTableRow,color=#00C2E0,inputs=1,outputs=1"`

    InClientID  runtime.InVariable[string] `spec:"title=Client ID,type=string,scope=Message,name=clientId,messageScope,jsScope,customScope"`
    InTableName runtime.InVariable[string] `spec:"title=Table Name,type=string,scope=Message,name=tableName,messageScope,jsScope,customScope"`
    InRowID     runtime.InVariable[string] `spec:"title=Row ID,type=string,scope=Message,name=rowId,messageScope,jsScope,customScope"`

    OptViewName runtime.OptVariable[string] `spec:"title=View Name,type=string,scope=Message,name=viewName,messageScope,customScope,jsScope"`
    OptConvert  runtime.OptVariable[bool]   `spec:"title=Convert Keys,type=bool,value=true,scope=Message,name=convertKeys,messageScope,customScope,jsScope"`

    OutStatusCode runtime.OutVariable[int]    `spec:"title=Status Code,type=int,scope=Message,name=statusCode,messageScope"`
    OutRaw        runtime.OutVariable[string] `spec:"title=Raw Body,type=string,scope=Message,name=body,messageScope"`
    OutJSON       runtime.OutVariable[any]    `spec:"title=JSON,type=object,scope=Message,name=json,messageScope"`
    OutRow        runtime.OutVariable[any]    `spec:"title=Row,type=object,scope=Message,name=row,messageScope"`
}

func (n *SeaTableGetRow) OnCreate() error { return nil }
func (n *SeaTableGetRow) OnClose() error  { return nil }

func (n *SeaTableGetRow) OnMessage(ctx message.Context) error {
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

    rowID, err := n.InRowID.Get(ctx)
    if err != nil {
        return err
    }
    rowID = strings.TrimSpace(rowID)
    if rowID == "" {
        return runtime.NewError("ErrInvalidArg", "Row ID is required")
    }

    viewName, _ := n.OptViewName.Get(ctx)
    convert, _ := n.OptConvert.Get(ctx)

    u, err := url.Parse(fmt.Sprintf("%s/api-gateway/api/v2/dtables/%s/rows/%s/", cfg.Server, cfg.BaseUUID, rowID))
    if err != nil {
        return fmt.Errorf("parse Get Row URL: %w", err)
    }
    q := u.Query()
    q.Set("table_name", tableName)
    if strings.TrimSpace(viewName) != "" {
        q.Set("view_name", viewName)
    }
    if convert {
        q.Set("convert_keys", "true")
    }
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

    row := parsed
    if m, ok := parsed.(map[string]any); ok {
        if v, ok := m["row"]; ok {
            row = v
        }
    }
    n.OutRow.Set(ctx, row)
    return nil
}
