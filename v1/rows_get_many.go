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

// SeaTableRowsGetMany paginates List Rows to collect many rows.
type SeaTableRowsGetMany struct {
    runtime.Node `spec:"id=Robomotion.SeaTable.RowsGetMany,name=Rows Get Many,icon=mdiTableMultiple,color=#00C2E0,inputs=1,outputs=1"`

    InClientID runtime.InVariable[string] `spec:"title=Client ID,type=string,scope=Message,name=clientId,messageScope,jsScope,customScope"`
    InTableName runtime.InVariable[string] `spec:"title=Table Name,type=string,scope=Message,name=tableName,messageScope,jsScope,customScope"`

    OptViewName   runtime.OptVariable[string] `spec:"title=View Name,type=string,scope=Message,name=viewName,messageScope,customScope,jsScope"`
    OptStart      runtime.OptVariable[int]    `spec:"title=Start Offset,type=int,value=0,scope=Message,name=start,messageScope,customScope,jsScope"`
    OptPageSize   runtime.OptVariable[int]    `spec:"title=Page Size,type=int,value=1000,scope=Message,name=pageSize,messageScope,customScope,jsScope"`
    OptMaxRows    runtime.OptVariable[int]    `spec:"title=Max Rows,type=int,value=10000,scope=Message,name=maxRows,messageScope,customScope,jsScope"`
    OptConvert    runtime.OptVariable[bool]   `spec:"title=Convert Keys,type=bool,value=true,scope=Message,name=convertKeys,messageScope,customScope,jsScope"`

    OutStatusCode runtime.OutVariable[int]    `spec:"title=Status Code,type=int,scope=Message,name=statusCode,messageScope"`
    OutRows       runtime.OutVariable[any]    `spec:"title=Rows,type=object,scope=Message,name=rows,messageScope"`
    OutJSON       runtime.OutVariable[any]    `spec:"title=JSON,type=object,scope=Message,name=json,messageScope"`
}

func (n *SeaTableRowsGetMany) OnCreate() error { return nil }
func (n *SeaTableRowsGetMany) OnClose() error  { return nil }

func (n *SeaTableRowsGetMany) OnMessage(ctx message.Context) error {
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

    start, _ := n.OptStart.Get(ctx)
    if start < 0 {
        start = 0
    }
    pageSize, _ := n.OptPageSize.Get(ctx)
    if pageSize <= 0 || pageSize > 1000 {
        pageSize = 1000
    }
    maxRows, _ := n.OptMaxRows.Get(ctx)
    if maxRows <= 0 {
        maxRows = 10000
    }
    convert, _ := n.OptConvert.Get(ctx)
    viewName, _ := n.OptViewName.Get(ctx)

    allRows := make([]any, 0, pageSize)
    statusCode := 0
    fetched := 0
    offset := start

    goCtx := context.Background()

    for {
        if fetched >= maxRows {
            break
        }

        u, err := url.Parse(fmt.Sprintf("%s/api-gateway/api/v2/dtables/%s/rows/", cfg.Server, cfg.BaseUUID))
        if err != nil {
            return fmt.Errorf("parse rows URL: %w", err)
        }
        q := u.Query()
        q.Set("table_name", tableName)
        if strings.TrimSpace(viewName) != "" {
            q.Set("view_name", viewName)
        }
        q.Set("start", strconv.Itoa(offset))

        remaining := maxRows - fetched
        limit := pageSize
        if remaining < limit {
            limit = remaining
        }
        q.Set("limit", strconv.Itoa(limit))
        if convert {
            q.Set("convert_keys", "true")
        }
        u.RawQuery = q.Encode()

        respBody, sc, err := doSeaTableRequest(goCtx, "GET", u.String(), cfg.Token, nil)
        if err != nil {
            return err
        }
        statusCode = sc

        var parsed any
        if err := json.Unmarshal(respBody, &parsed); err != nil {
            return fmt.Errorf("unmarshal list rows response: %w", err)
        }

        var rowsPage []any
        switch t := parsed.(type) {
        case map[string]any:
            if v, ok := t["rows"]; ok {
                if arr, ok := v.([]any); ok {
                    rowsPage = arr
                }
            }
        case []any:
            rowsPage = t
        }

        if len(rowsPage) == 0 {
            break
        }

        allRows = append(allRows, rowsPage...)
        fetched += len(rowsPage)
        offset += len(rowsPage)

        if len(rowsPage) < limit {
            break
        }
    }

    n.OutStatusCode.Set(ctx, statusCode)
    n.OutRows.Set(ctx, allRows)
    result := map[string]any{
        "rows":  allRows,
        "count": len(allRows),
        "start": start,
    }
    n.OutJSON.Set(ctx, result)
    return nil
}
