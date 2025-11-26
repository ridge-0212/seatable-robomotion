package v1

import (
    "context"
    "encoding/json"
    "fmt"
    "net/url"
    "strconv"
    "strings"

    "github.com/robomotionio/robomotion-go/message"
    "github.com/robomotionio/robomotion-go/runtime"
)

// SeaTableRows provides list / append / update / delete for rows.
type SeaTableRows struct {
    runtime.Node `spec:"id=SeaTable.Rows,name=Rows,icon=mdiTable,color=#00C2E0,inputs=1,outputs=1"`

    InClientID runtime.InVariable[string] `spec:"title=Client ID,type=string,scope=Message,name=clientId,messageScope,jsScope,customScope"`
    OptAction  string                     `spec:"title=Action,value=list,enum=list|append|update|delete,enumNames=List|Append|Update|Delete,option"`

    InTableName runtime.InVariable[string]      `spec:"title=Table Name,type=string,scope=Message,name=tableName,messageScope,jsScope,customScope"`
    OptViewName runtime.OptVariable[string]     `spec:"title=View Name,type=string,scope=Message,name=viewName,messageScope,customScope,jsScope"`
    OptStart    runtime.OptVariable[int]        `spec:"title=Start,type=int,value=0,scope=Message,name=start,messageScope,customScope,jsScope"`
    OptLimit    runtime.OptVariable[int]        `spec:"title=Limit,type=int,value=1000,scope=Message,name=limit,messageScope,customScope,jsScope"`
    OptConvert  runtime.OptVariable[bool]       `spec:"title=Convert Keys,type=bool,value=true,scope=Message,name=convertKeys,messageScope,customScope,jsScope"`
    OptRowID    runtime.OptVariable[string]     `spec:"title=Row ID,type=string,scope=Message,name=rowId,messageScope,customScope,jsScope"`
    OptRowData  runtime.OptVariable[any]        `spec:"title=Row Data,type=object,scope=Message,name=rowData,messageScope,customScope,jsScope"`

    OutStatusCode runtime.OutVariable[int]         `spec:"title=Status Code,type=int,scope=Message,name=statusCode,messageScope"`
    OutRaw        runtime.OutVariable[string]      `spec:"title=Raw Body,type=string,scope=Message,name=body,messageScope"`
    OutJSON       runtime.OutVariable[any]         `spec:"title=JSON,type=object,scope=Message,name=json,messageScope"`
}

func (n *SeaTableRows) OnCreate() error { return nil }
func (n *SeaTableRows) OnClose() error  { return nil }

func (n *SeaTableRows) OnMessage(ctx message.Context) error {
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

    action := n.OptAction
    if action == "" {
        action = "list"
    }

    var (
        method   = "GET"
        urlStr   string
        bodyData any
    )

    switch action {
    case "list":
        u, err := url.Parse(fmt.Sprintf("%s/api-gateway/api/v2/dtables/%s/rows/", cfg.Server, cfg.BaseUUID))
        if err != nil {
            return err
        }
        q := u.Query()
        q.Set("table_name", tableName)

        if v, err := n.OptViewName.Get(ctx); err == nil && strings.TrimSpace(v) != "" {
            q.Set("view_name", v)
        }
        if s, err := n.OptStart.Get(ctx); err == nil && s > 0 {
            q.Set("start", strconv.Itoa(s))
        }
        if l, err := n.OptLimit.Get(ctx); err == nil && l > 0 {
            q.Set("limit", strconv.Itoa(l))
        }
        if c, err := n.OptConvert.Get(ctx); err == nil && c {
            q.Set("convert_keys", "true")
        }
        u.RawQuery = q.Encode()
        urlStr = u.String()
        method = "GET"
        bodyData = nil

    case "append":
        method = "POST"
        rowData, err := n.OptRowData.Get(ctx)
        if err != nil || rowData == nil {
            return runtime.NewError("ErrInvalidArg", "Row Data is required for append")
        }
        urlStr = fmt.Sprintf("%s/api-gateway/api/v2/dtables/%s/rows/", cfg.Server, cfg.BaseUUID)
        bodyData = map[string]any{
            "table_name": tableName,
            "row":        rowData,
        }

    case "update":
        method = "PUT"
        rowID, err := n.OptRowID.Get(ctx)
        if err != nil || strings.TrimSpace(rowID) == "" {
            return runtime.NewError("ErrInvalidArg", "Row ID is required for update")
        }
        rowData, err := n.OptRowData.Get(ctx)
        if err != nil || rowData == nil {
            return runtime.NewError("ErrInvalidArg", "Row Data is required for update")
        }
        urlStr = fmt.Sprintf("%s/api-gateway/api/v2/dtables/%s/rows/", cfg.Server, cfg.BaseUUID)
        bodyData = map[string]any{
            "table_name": tableName,
            "row_id":     rowID,
            "row":        rowData,
        }

    case "delete":
        method = "DELETE"
        rowID, err := n.OptRowID.Get(ctx)
        if err != nil || strings.TrimSpace(rowID) == "" {
            return runtime.NewError("ErrInvalidArg", "Row ID is required for delete")
        }
        urlStr = fmt.Sprintf("%s/api-gateway/api/v2/dtables/%s/rows/", cfg.Server, cfg.BaseUUID)
        bodyData = map[string]any{
            "table_name": tableName,
            "row_id":     rowID,
        }

    default:
        return runtime.NewError("ErrInvalidArg", "Unsupported action for Rows")
    }

    respBody, status, err := doSeaTableRequest(context.Background(), method, urlStr, cfg.Token, bodyData)
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
