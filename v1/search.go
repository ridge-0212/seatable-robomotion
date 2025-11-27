package v1

import (
    "context"
    "encoding/json"
    "fmt"
    "strings"

    "github.com/robomotionio/robomotion-go/message"
    "github.com/robomotionio/robomotion-go/runtime"
)

// SeaTableSearch builds a SQL query to search keyword across multiple columns.
type SeaTableSearch struct {
    runtime.Node `spec:"id=Robomotion.SeaTable.Search,name=Search,icon=mdiDatabaseSearch,color=#00C2E0,inputs=1,outputs=1"`

    InClientID  runtime.InVariable[string] `spec:"title=Client ID,type=string,scope=Message,name=clientId,messageScope,jsScope,customScope"`
    InTableName runtime.InVariable[string] `spec:"title=Table Name,type=string,scope=Message,name=tableName,messageScope,jsScope,customScope"`
    InColumns   runtime.InVariable[string] `spec:"title=Columns (comma separated),type=string,scope=Message,name=columns,messageScope,jsScope,customScope"`
    InKeyword   runtime.InVariable[string] `spec:"title=Keyword,type=string,scope=Message,name=keyword,messageScope,jsScope,customScope"`

    OptMatchMode string                   `spec:"title=Match Mode,value=contains,enum=contains|equals|startsWith|endsWith,enumNames=Contains|Equals|Starts With|Ends With,option"`
    OptCaseSensitive runtime.OptVariable[bool] `spec:"title=Case Sensitive,type=bool,value=false,scope=Message,name=caseSensitive,messageScope,customScope,jsScope"`
    OptMaxRows       runtime.OptVariable[int]  `spec:"title=Max Rows,type=int,value=100,scope=Message,name=maxRows,messageScope,customScope,jsScope"`
    OptConvert       runtime.OptVariable[bool] `spec:"title=Convert Keys,type=bool,value=true,scope=Message,name=convertKeys,messageScope,customScope,jsScope"`

    OutStatusCode runtime.OutVariable[int]    `spec:"title=Status Code,type=int,scope=Message,name=statusCode,messageScope"`
    OutRaw        runtime.OutVariable[string] `spec:"title=Raw Body,type=string,scope=Message,name=body,messageScope"`
    OutJSON       runtime.OutVariable[any]    `spec:"title=JSON,type=object,scope=Message,name=json,messageScope"`
    OutRows       runtime.OutVariable[any]    `spec:"title=Rows,type=object,scope=Message,name=rows,messageScope"`
    OutCount      runtime.OutVariable[int]    `spec:"title=Count,type=int,scope=Message,name=count,messageScope"`
}

func (n *SeaTableSearch) OnCreate() error { return nil }
func (n *SeaTableSearch) OnClose() error  { return nil }

func (n *SeaTableSearch) OnMessage(ctx message.Context) error {
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

    columnsStr, err := n.InColumns.Get(ctx)
    if err != nil {
        return err
    }
    cols := splitColumns(columnsStr)
    if len(cols) == 0 {
        return runtime.NewError("ErrInvalidArg", "At least one column is required")
    }

    keyword, err := n.InKeyword.Get(ctx)
    if err != nil {
        return err
    }
    keyword = strings.TrimSpace(keyword)
    if keyword == "" {
        return runtime.NewError("ErrInvalidArg", "Keyword is required")
    }

    matchMode := n.OptMatchMode
    if matchMode == "" {
        matchMode = "contains"
    }

    caseSensitive, _ := n.OptCaseSensitive.Get(ctx)
    maxRows, _ := n.OptMaxRows.Get(ctx)
    if maxRows <= 0 {
        maxRows = 100
    }
    convert, _ := n.OptConvert.Get(ctx)

    var conditions []string
    var params []any

    for _, col := range cols {
        if !caseSensitive {
            switch matchMode {
            case "equals":
                conditions = append(conditions, fmt.Sprintf("LOWER(%s) = ?", col))
                params = append(params, strings.ToLower(keyword))
            case "startsWith":
                conditions = append(conditions, fmt.Sprintf("LOWER(%s) LIKE ?", col))
                params = append(params, strings.ToLower(keyword)+"%")
            case "endsWith":
                conditions = append(conditions, fmt.Sprintf("LOWER(%s) LIKE ?", col))
                params = append(params, "%"+strings.ToLower(keyword))
            default:
                conditions = append(conditions, fmt.Sprintf("LOWER(%s) LIKE ?", col))
                params = append(params, "%"+strings.ToLower(keyword)+"%")
            }
        } else {
            switch matchMode {
            case "equals":
                conditions = append(conditions, fmt.Sprintf("%s = ?", col))
                params = append(params, keyword)
            case "startsWith":
                conditions = append(conditions, fmt.Sprintf("%s LIKE ?", col))
                params = append(params, keyword+"%")
            case "endsWith":
                conditions = append(conditions, fmt.Sprintf("%s LIKE ?", col))
                params = append(params, "%"+keyword)
            default:
                conditions = append(conditions, fmt.Sprintf("%s LIKE ?", col))
                params = append(params, "%"+keyword+"%")
            }
        }
    }

    whereClause := strings.Join(conditions, " OR ")
    sqlText := fmt.Sprintf("SELECT * FROM %s WHERE %s LIMIT %d", tableName, whereClause, maxRows)

    body := map[string]any{
        "sql":          sqlText,
        "params":       params,
        "convert_keys": convert,
    }

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

    var rows []any
    if m, ok := parsed.(map[string]any); ok {
        if v, ok := m["results"]; ok {
            if arr, ok := v.([]any); ok {
                rows = arr
            }
        }
    }
    if rows == nil {
        rows = []any{}
    }
    n.OutRows.Set(ctx, rows)
    n.OutCount.Set(ctx, len(rows))

    return nil
}

func splitColumns(s string) []string {
    parts := strings.Split(s, ",")
    cols := make([]string, 0, len(parts))
    for _, p := range parts {
        c := strings.TrimSpace(p)
        if c != "" {
            cols = append(cols, c)
        }
    }
    return cols
}
